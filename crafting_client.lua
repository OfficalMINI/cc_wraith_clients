-- =============================================
-- WRAITH OS - CRAFTING CLIENT
-- =============================================
-- Run on crafty turtles connected via wired modem.
-- Registers with Wraith OS and executes craft commands.
--
-- Setup: Place crafty turtle on wired modem network.
--        Run: crafting_client

local CLIENT_TYPE = "crafting_client"

-- Compute version hash from own file content (must match updater_svc algorithm)
local function compute_version()
    local path = shell.getRunningProgram()
    local f = fs.open(path, "r")
    if not f then return "0" end
    local content = f.readAll()
    f.close()
    local sum = 0
    for i = 1, #content do
        sum = (sum * 31 + string.byte(content, i)) % 2147483647
    end
    return tostring(sum)
end
local VERSION = compute_version()
local WRAITH_ID = nil

local PROTOCOLS = {
    ping    = "wraith_craft_ping",
    status  = "wraith_craft_status",
    command = "wraith_craft_cmd",
    result  = "wraith_craft_result",
}

local UPDATE_PROTO = {
    ping = "wraith_update_ping",
    push = "wraith_update_push",
    ack  = "wraith_update_ack",
}

local HEARTBEAT_INTERVAL = 5
local DISCOVERY_INTERVAL = 10
local DISCOVERY_TIMEOUT = 3

-- ========================================
-- Verify Crafty Turtle
-- ========================================
if not turtle then
    printError("This script must run on a turtle!")
    return
end

if not turtle.craft then
    printError("This turtle needs a crafting table!")
    printError("Use a crafty turtle (craft turtle + crafting table)")
    return
end

-- ========================================
-- Modem Setup
-- ========================================
local function find_modem()
    -- Prefer wireless modem for rednet communication
    for _, side in ipairs({"back", "top", "left", "right", "bottom", "front"}) do
        if peripheral.getType(side) == "modem" then
            local m = peripheral.wrap(side)
            if m and m.isWireless and m.isWireless() then
                return side
            end
        end
    end
    -- Fall back to any modem
    for _, side in ipairs({"back", "top", "left", "right", "bottom", "front"}) do
        if peripheral.getType(side) == "modem" then return side end
    end
    return nil
end

local modem_side = find_modem()
if not modem_side then
    printError("No modem found! Attach a wired modem.")
    return
end
rednet.open(modem_side)

-- Get our local name on the wired network (for pushItems/pullItems)
-- Must find a WIRED modem — the rednet modem may be wireless
local LOCAL_NAME = nil
for _, side in ipairs({"back", "top", "left", "right", "bottom", "front"}) do
    if peripheral.getType(side) == "modem" then
        local m = peripheral.wrap(side)
        if m and m.getNameLocal then
            local name = m.getNameLocal()
            if name then
                LOCAL_NAME = name
                break
            end
        end
    end
end

-- Set label if not set
if not os.getComputerLabel() then
    os.setComputerLabel("Wraith Crafter " .. os.getComputerID())
end

print("Wraith Crafting Client v" .. VERSION)
print("Computer ID: " .. os.getComputerID())
print("Label: " .. (os.getComputerLabel() or "none"))
print("Modem: " .. modem_side)
if LOCAL_NAME then
    print("Wired name: " .. LOCAL_NAME)
else
    printError("WARNING: No wired modem name found!")
    printError("Item return to storage will not work.")
end
print()

-- ========================================
-- Update Check
-- ========================================
local function check_for_updates()
    print("Checking for updates...")
    rednet.broadcast(
        {client_type = CLIENT_TYPE, version = VERSION},
        UPDATE_PROTO.ping
    )
    local sender, msg = rednet.receive(UPDATE_PROTO.push, 3)
    if msg and type(msg) == "table" and msg.content then
        print("Update received! Installing...")
        local path = shell.getRunningProgram()
        local f = fs.open(path, "w")
        if f then
            f.write(msg.content)
            f.close()
            rednet.send(sender, {client_type = CLIENT_TYPE}, UPDATE_PROTO.ack)
            print("Update installed. Rebooting...")
            sleep(0.5)
            os.reboot()
        end
    else
        print("No updates available.")
    end
end

check_for_updates()

-- ========================================
-- Turtle Inventory Helpers
-- ========================================

-- Grid slot (1-9) to turtle slot mapping
local GRID_TO_TURTLE = {1, 2, 3, 5, 6, 7, 9, 10, 11}

-- Non-grid slots (for output / spillover)
local FREE_SLOTS = {4, 8, 12, 13, 14, 15, 16}

local function get_inventory()
    local inv = {}
    for slot = 1, 16 do
        local detail = turtle.getItemDetail(slot)
        if detail then
            inv[slot] = {
                name = detail.name,
                count = detail.count,
                nbt = detail.nbt,
                displayName = detail.displayName or detail.name,
            }
        end
    end
    return inv
end

local function clear_inventory()
    -- Try to push everything out via peripheral (to adjacent inventory)
    -- If no adjacent inventory, just consolidate to free slots
    for slot = 1, 16 do
        if turtle.getItemCount(slot) > 0 then
            turtle.select(slot)
            -- Try dropping into attached inventory
            local dropped = false
            for _, dir in ipairs({"drop", "dropUp", "dropDown"}) do
                if turtle[dir]() then
                    dropped = true
                    break
                end
            end
            if not dropped then
                -- Move to a free slot if in grid area
                for _, fs in ipairs(FREE_SLOTS) do
                    if turtle.getItemCount(fs) == 0 then
                        turtle.transferTo(fs)
                        break
                    end
                end
            end
        end
    end
end

-- ========================================
-- Command Handlers
-- ========================================

local function push_items_to_storage(storage_names)
    if not LOCAL_NAME then
        print("  No wired name - can't return items")
        return 0, 0
    end
    if not storage_names or #storage_names == 0 then
        print("  No storage names provided")
        return 0, 0
    end
    local returned = 0
    local stuck = 0
    for slot = 1, 16 do
        -- Loop until slot is fully empty (handles partial pulls from near-full chests)
        while turtle.getItemCount(slot) > 0 do
            local pulled_any = false
            for _, store_name in ipairs(storage_names) do
                local p_ok, pulled = pcall(peripheral.call, store_name, "pullItems", LOCAL_NAME, slot)
                if p_ok and pulled and pulled > 0 then
                    returned = returned + pulled
                    pulled_any = true
                    break
                end
            end
            if not pulled_any then
                stuck = stuck + turtle.getItemCount(slot)
                break
            end
        end
    end
    return returned, stuck
end

local function handle_craft(msg)
    local count = msg.count or 1
    local storage_names = msg.storage_names or {}
    print("Crafting x" .. count .. "...")

    -- Select output slot (first free slot)
    for _, slot in ipairs(FREE_SLOTS) do
        if turtle.getItemCount(slot) == 0 then
            turtle.select(slot)
            break
        end
    end

    local ok = turtle.craft(count)
    local items_returned = 0
    local items_stuck = 0

    if ok then
        print("  Crafted " .. count .. " successfully")
        -- Push crafted items back to storage
        items_returned, items_stuck = push_items_to_storage(storage_names)
        if items_returned > 0 then
            print("  Returned " .. items_returned .. " items to storage")
        end
        if items_stuck > 0 then
            print("  WARNING: " .. items_stuck .. " items stuck on turtle!")
        end
    else
        print("  Craft FAILED")
        -- Push ingredients back to storage on failure too
        items_returned, items_stuck = push_items_to_storage(storage_names)
        if items_returned > 0 then
            print("  Returned " .. items_returned .. " ingredients to storage")
        end
    end

    return {
        action = "craft_result",
        success = ok,
        crafted = ok and count or 0,
        items_returned = items_returned,
        items_stuck = items_stuck,
    }
end

local function handle_clear()
    print("Clearing inventory...")
    clear_inventory()
    return {action = "clear_result", success = true}
end

local function handle_status()
    return {
        action = "status_result",
        fuel = turtle.getFuelLevel(),
        fuel_limit = turtle.getFuelLimit(),
        label = os.getComputerLabel(),
        id = os.getComputerID(),
        slots = get_inventory(),
    }
end

-- ========================================
-- Discovery
-- ========================================

local function discover_wraith()
    rednet.broadcast(
        {
            type = "crafty",
            label = os.getComputerLabel(),
            id = os.getComputerID(),
        },
        PROTOCOLS.ping
    )

    local sender, msg = rednet.receive(PROTOCOLS.status, DISCOVERY_TIMEOUT)
    if sender then
        WRAITH_ID = sender
        print("Connected to Wraith OS (ID: " .. sender .. ")")
        return true
    end
    return false
end

-- ========================================
-- Main Loops
-- ========================================

local function command_listener()
    while true do
        local sender, msg, proto = rednet.receive(PROTOCOLS.command)
        if sender and type(msg) == "table" and msg.action then
            local result

            if msg.action == "craft" then
                result = handle_craft(msg)
            elseif msg.action == "clear" then
                result = handle_clear()
            elseif msg.action == "status" then
                result = handle_status()
            else
                result = {action = "error", message = "Unknown action: " .. tostring(msg.action)}
            end

            if result then
                rednet.send(sender, result, PROTOCOLS.result)
            end
        end
    end
end

local function heartbeat_sender()
    while true do
        sleep(HEARTBEAT_INTERVAL)
        if WRAITH_ID then
            rednet.send(WRAITH_ID,
                {
                    action = "heartbeat",
                    id = os.getComputerID(),
                    label = os.getComputerLabel(),
                    fuel = turtle.getFuelLevel(),
                },
                PROTOCOLS.status
            )
        end
    end
end

local function discovery_loop()
    -- Initial discovery
    print("Discovering Wraith OS...")
    while not WRAITH_ID do
        if discover_wraith() then break end
        print("  Retrying in " .. DISCOVERY_INTERVAL .. "s...")
        sleep(DISCOVERY_INTERVAL)
    end

    -- Periodic re-discovery
    local missed_pings = 0
    while true do
        sleep(60)
        if WRAITH_ID then
            -- Verify connection with ping
            rednet.send(WRAITH_ID,
                {type = "crafty", label = os.getComputerLabel(), id = os.getComputerID()},
                PROTOCOLS.ping
            )
            local _, resp = rednet.receive(PROTOCOLS.status, 5)
            if not resp then
                missed_pings = missed_pings + 1
                if missed_pings >= 3 then
                    print("Lost connection to Wraith (3 missed pings). Rediscovering...")
                    WRAITH_ID = nil
                    missed_pings = 0
                    while not WRAITH_ID do
                        if discover_wraith() then break end
                        sleep(DISCOVERY_INTERVAL)
                    end
                else
                    print("Ping missed (" .. missed_pings .. "/3), retrying...")
                end
            else
                missed_pings = 0
            end
        end
    end
end

-- ========================================
-- Entry Point
-- ========================================

print()
print("Starting crafting client...")
print("Press Ctrl+T to terminate")
print()

local function update_checker()
    -- Periodically check for updates (every 5 minutes)
    while true do
        sleep(300)
        check_for_updates()
    end
end

parallel.waitForAny(
    command_listener,
    heartbeat_sender,
    discovery_loop,
    update_checker
)
