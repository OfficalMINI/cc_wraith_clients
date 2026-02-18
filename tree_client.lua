-- =============================================
-- WRAITH OS - TREE FARM CLIENT
-- =============================================
-- Adapted from aTreeFarm by Kaikaku (v1.04).
-- Runs on crafty felling turtles. Communicates
-- with Wraith via adjacent wired modem between rounds.
-- Self-recovering: no UI prompts, auto-restarts on stuck.
--
-- First run: tree_client setup   (builds farm from placement position)
-- After:     tree_client         (auto-farms, comms via adjacent modem)

local CLIENT_TYPE = "tree_client"
local UPDATE_URL = "https://raw.githubusercontent.com/OfficalMINI/cc_wraith_tree_client/refs/heads/main/tree_client.lua"

-- Version hash from own file content
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

-- Raw modem channels (no rednet — turtle has no modem peripheral)
local CHANNELS = {
    ping    = 7401,
    status  = 7402,
    command = 7403,
    result  = 7404,
}

-- ========================================
-- Verify Turtle
-- ========================================
if not turtle then
    printError("This script must run on a turtle!")
    return
end
if not turtle.craft then
    printError("Need a crafty turtle (crafting table equipped)")
    return
end

-- ========================================
-- Constants (from original aTreeFarm)
-- ========================================
local MIN_FUEL       = 960 * 2    -- 2 stacks of planks
local SLOT_SAPLING   = 1
local SLOT_WOOD      = 2          -- comparison wood
local SLOT_CHEST     = 16         -- chest for crafty refuel
local SLOT_REFUEL    = 15
local CRAFT_FUEL_MAX = 32         -- max logs to craft into planks for fuel
local EXTRA_DIG_UP   = 1          -- extra levels for jungle branches
local LOOP_END       = 56         -- path loop length
local MAX_MOVE_TRIES = 20         -- timeout for stuck movement
local COMMS_TIMEOUT  = 3          -- seconds to listen for commands at modem
local PATH_COMMS_INTERVAL = 4    -- send heartbeat every N path steps (modem under every path block)
local INV_FULL_THRESHOLD = 2      -- skip cutting when this many free slots remain

-- Path encoding (from original — defines the patrol route)
local PATH_STR = "tReeTreESdig!diG;-)FaRmKaIKAKUudIgYdIgyTreEAndsOrRygUYsd"


-- ========================================
-- State
-- ========================================
local CONFIG_FILE = "tree_farm.cfg"
local cfg = {
    home_set = false,
    running = true,
    loop_count = 0,       -- 0 = infinite
}

local round_counter = 0
local current_state = "idle"    -- idle | farming | refueling | setup | stuck | comms
local farm_running = true       -- controlled by start/stop commands
local last_activity = os.clock()

-- ========================================
-- Config Persistence
-- ========================================
local function save_cfg()
    local ok, content = pcall(textutils.serialise, cfg)
    if not ok or not content or #content < 5 then return end
    local tmp = CONFIG_FILE .. ".tmp"
    local f = fs.open(tmp, "w")
    if f then
        f.write(content)
        f.close()
        if fs.exists(CONFIG_FILE) then fs.delete(CONFIG_FILE) end
        fs.move(tmp, CONFIG_FILE)
    end
end

local function load_cfg()
    if fs.exists(CONFIG_FILE) then
        local f = fs.open(CONFIG_FILE, "r")
        if f then
            local data = f.readAll()
            f.close()
            local parsed = textutils.unserialise(data)
            if type(parsed) == "table" then
                for k, v in pairs(parsed) do cfg[k] = v end
            end
        end
    end
end

-- ========================================
-- Modem Protection
-- ========================================
local function is_modem_block(inspect_fn)
    local ok, info = inspect_fn()
    if ok and info and type(info) == "table" and info.name then
        return info.name:find("modem") ~= nil
    end
    return false
end

-- ========================================
-- Block Identification (multi-turtle safety)
-- ========================================
local function is_tree_block()
    local ok, info = turtle.inspect()
    if not ok or not info then return false end
    local name = info.name or ""
    return name:find("log") ~= nil or name:find("wood") ~= nil
        or name:find("stem") ~= nil or name:find("hyphae") ~= nil
end

local function is_turtle_block(inspect_fn)
    local ok, info = (inspect_fn or turtle.inspect)()
    if not ok or not info then return false end
    local name = info.name or ""
    return name:find("turtle") ~= nil
end

local function is_container_below()
    local ok, info = turtle.inspectDown()
    if not ok or not info then return false end
    local name = info.name or ""
    return name:find("chest") ~= nil or name:find("barrel") ~= nil
        or name:find("shulker") ~= nil or name:find("crate") ~= nil
        or name:find("drawer") ~= nil or name:find("hopper") ~= nil
end

-- Check if a block should NEVER be dug (modem, turtle, or container)
-- Path is wired modems — already covered by "modem" check
local function is_protected(inspect_fn)
    local ok, info = inspect_fn()
    if not ok or not info then return false end
    local name = info.name or ""
    return name:find("modem") ~= nil or name:find("turtle") ~= nil
        or name:find("chest") ~= nil or name:find("barrel") ~= nil
        or name:find("shulker") ~= nil or name:find("crate") ~= nil
        or name:find("drawer") ~= nil or name:find("hopper") ~= nil
end

-- ========================================
-- Movement (with timeout — never infinite loop)
-- ========================================
local function try_forward(max)
    max = max or MAX_MOVE_TRIES
    for i = 1, max do
        if turtle.forward() then return true end
        if is_protected(turtle.inspect) then
            -- Protected block (turtle/modem/chest) — just wait
            sleep(0.5)
        else
            turtle.dig()
            turtle.attack()
        end
        if i >= max then return false end
        sleep(0.2)
    end
    return false
end

local function try_back(max)
    max = max or MAX_MOVE_TRIES
    for i = 1, max do
        if turtle.back() then return true end
        if i >= max then return false end
        sleep(0.2)
    end
    return false
end

local function try_up(max)
    max = max or MAX_MOVE_TRIES
    for i = 1, max do
        if turtle.up() then return true end
        if is_protected(turtle.inspectUp) then
            -- Protected block above: random wait then bail to prevent deadlock
            sleep(0.5 + math.random() * 2)
            if turtle.up() then return true end
            return false
        else
            turtle.digUp()
            turtle.attackUp()
        end
        if i >= max then return false end
        sleep(0.2)
    end
    return false
end

local function try_down(max)
    max = max or MAX_MOVE_TRIES
    for i = 1, max do
        if turtle.down() then return true end
        if is_protected(turtle.inspectDown) then
            -- Protected block below: random wait then bail to prevent deadlock
            sleep(0.5 + math.random() * 2)
            if turtle.down() then return true end
            return false
        else
            turtle.digDown()
            turtle.attackDown()
        end
        if i >= max then return false end
        sleep(0.2)
    end
    return false
end

local function gl(n)
    for i = 1, (n or 1) do turtle.turnLeft() end
end

local function gr(n)
    for i = 1, (n or 1) do turtle.turnRight() end
end

-- Multi-step movement with timeout
local function gf(n)
    for i = 1, (n or 1) do
        if not try_forward() then return false end
    end
    return true
end

local function gb(n)
    for i = 1, (n or 1) do
        if not try_back() then return false end
    end
    return true
end

local function gu(n)
    for i = 1, (n or 1) do
        if not try_up() then return false end
    end
    return true
end

local function gd(n)
    for i = 1, (n or 1) do
        if not try_down() then return false end
    end
    return true
end

-- Shorthand dig/place/suck
local function df()  if is_protected(turtle.inspect) then return false end; return turtle.dig() end
local function du()  if is_protected(turtle.inspectUp) then return false end; return turtle.digUp() end
local function dd()  if is_protected(turtle.inspectDown) then return false end; return turtle.digDown() end
local function sf()  turtle.suck() end
local function su()  turtle.suckUp() end
local function sd()  while turtle.suckDown() do end end
local function ss(s) turtle.select(s) end
local function Dd(n) turtle.dropDown(n or 64) end
local function Du(n) turtle.dropUp(n or 64) end

-- Place forward, moving back between multiple placements
local function pf(n)
    n = n or 1
    for i = 1, n do
        if i ~= 1 then try_back() end
        turtle.place()
    end
end

-- ========================================
-- Inventory Organization
-- ========================================
-- Check if a slot contains a sapling
local function is_sapling_item(slot)
    local detail = turtle.getItemDetail(slot)
    if not detail then return false end
    return detail.name:find("sapling") ~= nil or detail.name:find("propagule") ~= nil
end

-- Check if a slot contains a wood/log item
local function is_wood_item(slot)
    local detail = turtle.getItemDetail(slot)
    if not detail then return false end
    local n = detail.name
    return n:find("log") ~= nil or n:find("wood") ~= nil
        or n:find("stem") ~= nil or n:find("hyphae") ~= nil
end

-- Check if a slot contains a chest
local function is_chest_item(slot)
    local detail = turtle.getItemDetail(slot)
    if not detail then return false end
    return detail.name:find("chest") ~= nil
end

-- Consolidate inventory: saplings → slot 1, wood → slot 2, chest → slot 16
local function organize_inventory()
    -- === CHEST → slot 16 ===
    -- If slot 16 has something that isn't a chest, move it out
    if turtle.getItemCount(SLOT_CHEST) > 0 and not is_chest_item(SLOT_CHEST) then
        for dest = 3, 14 do
            if turtle.getItemCount(dest) == 0 then
                ss(SLOT_CHEST)
                turtle.transferTo(dest)
                break
            end
        end
    end
    -- Find a chest in any other slot and move it to 16
    if turtle.getItemCount(SLOT_CHEST) == 0 then
        for i = 1, 15 do
            if turtle.getItemCount(i) > 0 and is_chest_item(i) then
                ss(i)
                turtle.transferTo(SLOT_CHEST)
                break  -- only need 1 chest
            end
        end
    end

    -- === SAPLINGS → slot 1 ===
    -- Move any non-saplings out of slot 1
    if turtle.getItemCount(SLOT_SAPLING) > 0 and not is_sapling_item(SLOT_SAPLING) then
        for dest = 3, 14 do
            if turtle.getItemCount(dest) == 0 then
                ss(SLOT_SAPLING)
                turtle.transferTo(dest)
                break
            end
        end
    end
    -- Move scattered saplings into slot 1
    for i = 3, 14 do
        if turtle.getItemCount(i) > 0 and is_sapling_item(i) then
            ss(i)
            turtle.transferTo(SLOT_SAPLING)
        end
    end

    -- === WOOD → slot 2 ===
    -- Move any non-wood out of slot 2
    if turtle.getItemCount(SLOT_WOOD) > 0 and not is_wood_item(SLOT_WOOD) then
        for dest = 3, 14 do
            if turtle.getItemCount(dest) == 0 then
                ss(SLOT_WOOD)
                turtle.transferTo(dest)
                break
            end
        end
    end
    -- Move scattered wood into slot 2
    for i = 3, 14 do
        if turtle.getItemCount(i) > 0 and is_wood_item(i) then
            ss(i)
            turtle.transferTo(SLOT_WOOD)
        end
    end
    ss(1)
end

-- ========================================
-- Modem Communication
-- ========================================
-- Find adjacent wired modem (turtle moves to modem position between rounds)
local function find_adjacent_modem()
    for _, side in ipairs({"front", "back", "left", "right", "top", "bottom"}) do
        local ptype = peripheral.getType(side)
        if ptype == "modem" then
            local m = peripheral.wrap(side)
            if m and not m.isWireless() then
                return m, side
            end
        end
    end
    return nil, nil
end

-- ========================================
-- Pastebin Auto-Update
-- ========================================
-- Fetches latest script from Pastebin, compares hash with local VERSION.
-- If different, overwrites self and reboots. Needs internet via wired modem.
local function check_pastebin_update()
    if not http then
        print("[update] HTTP API not available")
        return false
    end
    print("[update] Checking github...")
    local ok, resp, err = pcall(http.get, UPDATE_URL)
    if not ok then
        print("[update] Error: " .. tostring(resp))
        return false
    end
    if not resp then
        print("[update] Failed: " .. tostring(err))
        return false
    end
    local code = resp.getResponseCode()
    local content = resp.readAll()
    resp.close()
    if code ~= 200 then
        print("[update] HTTP " .. tostring(code))
        return false
    end
    if not content or #content < 100 then
        print("[update] Bad response (" .. #content .. "b)")
        return false
    end
    -- Compare hash
    local sum = 0
    for i = 1, #content do
        sum = (sum * 31 + string.byte(content, i)) % 2147483647
    end
    local remote_ver = tostring(sum)
    if remote_ver == VERSION then
        print("[update] Up to date (ver=" .. VERSION .. ")")
        return false
    end
    -- Different version — update
    print("[update] New version! " .. VERSION .. " -> " .. remote_ver .. " (" .. #content .. "b)")
    local path = shell.getRunningProgram()
    local f = fs.open(path, "w")
    if not f then
        print("[update] ERROR: can't write " .. path)
        return false
    end
    f.write(content)
    f.close()
    print("[update] Written. Rebooting...")
    sleep(0.5)
    os.reboot()
end

local function do_comms()
    local modem, side = find_adjacent_modem()
    local stepped = false

    -- Path is wired modems, so modem may be below. Also try stepping forward
    -- to reach a modem (home position: modem is 1 forward)
    if not modem then
        if turtle.forward() then
            stepped = true
            modem, side = find_adjacent_modem()
        end
    end
    if not modem then
        print("[do_comms] no modem found")
        if stepped then turtle.back() end
        return
    end

    print("[do_comms] found modem on " .. side .. (stepped and " (stepped)" or ""))
    current_state = "comms"
    last_activity = os.clock()

    -- Open channels
    modem.open(CHANNELS.status)
    modem.open(CHANNELS.command)
    modem.open(CHANNELS.result)

    -- Send ping/heartbeat
    print("[do_comms] TX heartbeat (ver=" .. VERSION .. ")")
    modem.transmit(CHANNELS.ping, CHANNELS.status, {
        type = "tree_farmer",
        action = "heartbeat",
        id = os.getComputerID(),
        label = os.getComputerLabel() or ("TreeFarm " .. os.getComputerID()),
        fuel = turtle.getFuelLevel(),
        fuel_limit = turtle.getFuelLimit(),
        rounds = round_counter,
        state = farm_running and "farming" or "idle",
        saplings = turtle.getItemCount(SLOT_SAPLING),
        version = VERSION,
        client_type = CLIENT_TYPE,
    })

    -- Listen for commands/updates for a few seconds
    local got_ack = false
    local got_update = false
    local timer_id = os.startTimer(COMMS_TIMEOUT)
    while true do
        local ev = {os.pullEvent()}
        if ev[1] == "timer" and ev[2] == timer_id then
            break  -- Comms window expired
        elseif ev[1] == "modem_message" then
            local recv_side, recv_ch, reply_ch, msg, dist = ev[2], ev[3], ev[4], ev[5], ev[6]
            print("[do_comms] RX ch=" .. tostring(recv_ch) .. " action=" .. tostring(type(msg) == "table" and msg.action or "?"))
            if type(msg) == "table" then
                if recv_ch == CHANNELS.command and msg.action
                    and (not msg.target_id or msg.target_id == os.getComputerID()) then
                    -- Handle commands
                    if msg.action == "start" then
                        farm_running = true
                        cfg.running = true
                        save_cfg()
                        print("[do_comms] CMD: start")
                        modem.transmit(CHANNELS.result, CHANNELS.command, {
                            action = "cmd_result", id = os.getComputerID(), result = "started",
                        })
                    elseif msg.action == "stop" then
                        farm_running = false
                        cfg.running = false
                        save_cfg()
                        print("[do_comms] CMD: stop")
                        modem.transmit(CHANNELS.result, CHANNELS.command, {
                            action = "cmd_result", id = os.getComputerID(), result = "stopped",
                        })
                    elseif msg.action == "status" then
                        print("[do_comms] CMD: status request")
                        modem.transmit(CHANNELS.result, CHANNELS.command, {
                            action = "status_result",
                            id = os.getComputerID(),
                            label = os.getComputerLabel(),
                            fuel = turtle.getFuelLevel(),
                            fuel_limit = turtle.getFuelLimit(),
                            rounds = round_counter,
                            state = farm_running and "farming" or "idle",
                            saplings = turtle.getItemCount(SLOT_SAPLING),
                            version = VERSION,
                        })
                    elseif msg.action == "update" and msg.content then
                        got_update = true
                        print("[do_comms] UPDATE received (" .. #msg.content .. " bytes)")
                        local path = shell.getRunningProgram()
                        local uf = fs.open(path, "w")
                        if uf then
                            uf.write(msg.content)
                            uf.close()
                            print("[do_comms] Written to " .. path)
                            modem.transmit(CHANNELS.result, CHANNELS.command, {
                                action = "update_ack", id = os.getComputerID(),
                            })
                            modem.closeAll()
                            if stepped then
                                for i = 1, MAX_MOVE_TRIES do
                                    if turtle.back() then break end
                                    sleep(0.3)
                                end
                            end
                            print("Rebooting...")
                            sleep(0.5)
                            os.reboot()
                        else
                            print("[do_comms] ERROR: can't write file!")
                        end
                    else
                        print("[do_comms] unknown cmd: " .. tostring(msg.action))
                    end
                elseif recv_ch == CHANNELS.status then
                    got_ack = true
                end
            end
        end
    end

    print("[do_comms] " .. (got_ack and "ACK" or "no ack") .. (got_update and " +UPDATE" or " no update"))
    modem.closeAll()

    -- Check Pastebin for updates (uses internet via wired modem network)
    if not got_update then
        pcall(check_pastebin_update)
    end

    -- Return to home position (retry to prevent drift)
    if stepped then
        for i = 1, MAX_MOVE_TRIES do
            if turtle.back() then break end
            sleep(0.3)
        end
    end
end

-- ========================================
-- Mid-patrol Communication (lightweight)
-- ========================================
-- Quick heartbeat + command check while walking the path.
-- At patrol height (home+1), descends 1 block to home+0 where
-- the modem at home-1 is visible via inspectDown/peripheral.
-- If already_at_ground is true, skip the descent (caller already lowered).
-- step: current path step (1..LOOP_END) for progress display
local function path_comms(step, already_at_ground)
    local descended = false

    -- Descend 1 block from patrol height
    if not already_at_ground then
        local ok = turtle.down()
        if not ok then
            print("  [comms] down failed")
            return
        end
        descended = true
    end

    -- Wait on the modem before trying to use it
    sleep(0.5)

    -- Log all peripherals we can see
    local sides = {"top","bottom","left","right","front","back"}
    local found_side = nil
    for _, s in ipairs(sides) do
        local pt = peripheral.getType(s)
        if pt then
            print("  [comms] " .. s .. "=" .. pt)
            if pt == "modem" then found_side = s end
        end
    end

    if not found_side then
        print("  [comms] no modem on any side")
        if descended then turtle.up() end
        return
    end

    local modem = peripheral.wrap(found_side)
    if not modem or modem.isWireless() then
        print("  [comms] modem on " .. found_side .. " is wireless or nil")
        if descended then turtle.up() end
        return
    end

    local progress = step and math.floor(step / LOOP_END * 100) or 0
    print("  [comms] TX step=" .. (step or "?") .. " " .. progress .. "% via " .. found_side)

    modem.open(CHANNELS.status)
    modem.open(CHANNELS.command)

    modem.transmit(CHANNELS.ping, CHANNELS.status, {
        type = "tree_farmer",
        action = "heartbeat",
        id = os.getComputerID(),
        label = os.getComputerLabel() or ("TreeFarm " .. os.getComputerID()),
        fuel = turtle.getFuelLevel(),
        fuel_limit = turtle.getFuelLimit(),
        rounds = round_counter,
        state = farm_running and "farming" or "idle",
        saplings = turtle.getItemCount(SLOT_SAPLING),
        progress = progress,
        version = VERSION,
        client_type = CLIENT_TYPE,
    })

    -- Listen 0.5s for ack + commands
    local got_ack = false
    local timer_id = os.startTimer(0.5)
    while true do
        local ev = {os.pullEvent()}
        if ev[1] == "timer" and ev[2] == timer_id then
            break
        elseif ev[1] == "modem_message" then
            local recv_side, recv_ch, reply_ch, msg = ev[2], ev[3], ev[4], ev[5]
            if type(msg) == "table" then
                if recv_ch == CHANNELS.status and msg.status == "wraith_tree_hub" then
                    got_ack = true
                elseif recv_ch == CHANNELS.command and msg.action
                    and (not msg.target_id or msg.target_id == os.getComputerID()) then
                    if msg.action == "start" then
                        farm_running = true; cfg.running = true; save_cfg()
                        print("  [comms] CMD: start")
                    elseif msg.action == "stop" then
                        farm_running = false; cfg.running = false; save_cfg()
                        print("  [comms] CMD: stop")
                    end
                end
            end
        end
    end

    modem.close(CHANNELS.status)
    modem.close(CHANNELS.command)
    print("  [comms] " .. (got_ack and "ACK" or "no reply"))

    if descended then turtle.up() end
end

-- ========================================
-- Fuel Management (self-recovering)
-- ========================================
local function craft_fuel()
    local fuel_items = turtle.getItemCount(SLOT_WOOD)

    if turtle.getFuelLevel() >= MIN_FUEL then return true end
    if turtle.getItemCount(SLOT_CHEST) ~= 1 then return false end
    if fuel_items <= 1 then return false end

    current_state = "refueling"
    print("Auto refuel (" .. turtle.getFuelLevel() .. "/" .. MIN_FUEL .. ")...")

    -- Store everything in chest above
    ss(SLOT_CHEST)
    local placed = false
    for attempt = 1, 5 do
        if not is_protected(turtle.inspectUp) then turtle.digUp() end
        if turtle.placeUp() then placed = true; break end
        sleep(0.5)
    end
    if not placed then
        print("  Can't place chest for refuel")
        return false
    end

    for i = 1, 15 do
        ss(i)
        if i == SLOT_WOOD then
            Du(math.max(1, turtle.getItemCount(SLOT_WOOD) - CRAFT_FUEL_MAX))
        else
            Du()
        end
    end

    -- Craft planks
    turtle.craft()

    -- Refuel all
    for i = 1, 16 do
        ss(i)
        turtle.refuel()
    end
    print("  Fuel: " .. turtle.getFuelLevel() .. "/" .. MIN_FUEL)

    -- Get items back
    ss(1)
    while turtle.suckUp() do end
    -- Break our own temp chest to reclaim it
    ss(SLOT_CHEST)
    turtle.digUp()  -- raw dig — this is our own placed chest, not a world chest
    ss(1)

    return turtle.getFuelLevel() >= MIN_FUEL
end

local function check_refuel()
    if turtle.getFuelLevel() >= MIN_FUEL then return true end

    -- Try to refuel from slot 15 first
    ss(SLOT_REFUEL)
    turtle.refuel()
    ss(1)
    if turtle.getFuelLevel() >= MIN_FUEL then return true end

    -- Try crafting fuel
    return craft_fuel()
end

-- ========================================
-- Tree Cutting (adapted from original)
-- ========================================
local function plant_tree()
    sf()  -- pick up any items on ground
    if turtle.getItemCount(SLOT_SAPLING) > 1 and is_sapling_item(SLOT_SAPLING) then
        ss(SLOT_SAPLING)
        for attempt = 1, 4 do
            if turtle.place() then break end
            sleep(0.5)
        end
    end
    ss(1)
end

local function cut_tree()
    local extra_dig = EXTRA_DIG_UP
    local up_count = 0
    local face = 0   -- -1=left, 1=right

    print("  Cutting tree...")
    last_activity = os.clock()

    -- Enter tree column
    df()
    try_forward()
    gr(); df(); gl(); df(); gl(); df()
    face = -1

    -- Cut upward
    repeat
        local dug_something = false
        du()
        try_up()
        -- Pick up items that fell from above
        turtle.suck(); turtle.suckUp(); turtle.suckDown()
        dug_something = df() or dug_something
        if face == -1 then
            gr()
            dug_something = df() or dug_something
            gr()
        elseif face == 1 then
            gl()
            dug_something = df() or dug_something
            gl()
        end
        face = face * -1
        dug_something = df() or dug_something
        up_count = up_count + 1
        last_activity = os.clock()

        if not (turtle.detectUp() or dug_something) then
            extra_dig = extra_dig - 1
        else
            extra_dig = EXTRA_DIG_UP
        end
    until extra_dig < 0

    -- Step off tree column
    if face == -1 then gl()
    elseif face == 1 then gr()
    end
    df()
    try_forward()
    gl()

    -- Descend — dig front and back only (don't cut into neighbors)
    face = 1
    for i = 1, up_count + 1 do
        dd(); df(); gl(2); df()
        face = face * -1
        try_down()
        -- Improved: suck items at each level while descending
        sf(); su()
    end
    if face == 1 then gl()
    elseif face == -1 then gr()
    end

    -- Ground sweep: thorough pickup around tree base + diagonals
    sleep(0.5)  -- let items settle
    local function suck_all()
        turtle.suck(); turtle.suckUp(); turtle.suckDown()
    end
    -- Cardinal: 2 blocks in each direction
    for sweep = 1, 4 do
        suck_all()
        if turtle.forward() then
            suck_all()
            if turtle.forward() then
                suck_all()
                turtle.back()
            end
            turtle.back()
        end
        gr()
    end
    -- Diagonals: step forward+turn into each corner
    for diag = 1, 4 do
        if turtle.forward() then
            gr()
            if turtle.forward() then
                suck_all()
                turtle.back()
            end
            gl()
            turtle.back()
        end
        gr()
    end
    suck_all()

    print("  Done!")

    -- Replant
    plant_tree()
    try_up()
    sd()
end

-- ========================================
-- Path Step (from original eS function)
-- ========================================
local function path_step(i)
    local ch = PATH_STR:sub(i, i)
    if ch == "a" then
        gl()
    elseif ch == "d" then
        gr()
    else
        try_forward()
        -- Improved: suck items while walking path
        sf()
    end
end

-- ========================================
-- Empty Turtle via Placed Chest Above
-- ========================================
-- Places the turtle's own chest above, drops items into it,
-- waits for the storage system to extract, then breaks the chest.
-- Never uses the depot chest below (items get instantly sucked away).
local function empty_turtle()
    -- Check if there's anything to deposit (slots 3-15, excess wood in 2)
    local has_items = false
    for i = 3, 15 do
        if turtle.getItemCount(i) > 0 then has_items = true; break end
    end
    if turtle.getItemCount(SLOT_WOOD) > 1 then has_items = true end
    if not has_items then return end  -- nothing to deposit

    print("  Emptying inventory")

    if turtle.getItemCount(SLOT_CHEST) < 1 then
        print("  WARNING: No chest in slot " .. SLOT_CHEST .. " — can't empty!")
        return
    end

    -- Place our chest above
    ss(SLOT_CHEST)
    local placed = false
    for attempt = 1, 5 do
        if not is_protected(turtle.inspectUp) then turtle.digUp() end
        if turtle.placeUp() then placed = true; break end
        sleep(0.5)
    end
    if not placed then
        print("  WARNING: Can't place chest above — skipping empty!")
        ss(1)
        return
    end

    -- Drop excess wood (keep 1 for comparison)
    ss(SLOT_WOOD)
    if turtle.getItemCount(SLOT_WOOD) > 1 then
        Du(turtle.getItemCount(SLOT_WOOD) - 1)
    end
    -- Drop everything from slots 3-15
    for i = 3, 15 do
        ss(i)
        Du()
    end

    -- Wait for storage system to extract items from the chest
    sleep(2)

    -- Break the chest to reclaim it
    ss(SLOT_CHEST)
    turtle.digUp()

    -- Verify we got the chest back
    if turtle.getItemCount(SLOT_CHEST) < 1 then
        print("  WARNING: Chest lost! Trying suckUp...")
        turtle.suckUp()
    end
    ss(1)
end

-- ========================================
-- Farm Setup (from original, one-time)
-- ========================================
local function run_setup()
    current_state = "setup"
    print("Setting up tree farm...")

    if not check_refuel() then
        print("Need fuel in slot " .. SLOT_REFUEL .. "!")
        return false
    end

    -- Verify materials: slot 3=chest(1), slot 4=cobble(47+), slot 5=torches(8+)
    if turtle.getItemCount(3) < 1 or turtle.getItemCount(4) < 47 or turtle.getItemCount(5) < 8 then
        print("Setup materials needed:")
        print("  Slot 3: chest   (1)")
        print("  Slot 4: cobble  (47)")
        print("  Slot 5: torches (8)")
        return false
    end

    -- Chest
    gf(3); gr(); gf(3); gl(); ss(3); dd(); turtle.placeDown()
    -- Path
    ss(4)
    for i = 1, 9 do gf(); dd(); turtle.placeDown() end; gr()
    for i = 1, 3 do gf(); dd(); turtle.placeDown() end; gr()
    for i = 1, 6 do gf(); dd(); turtle.placeDown() end; gl()
    for i = 1, 3 do gf(); dd(); turtle.placeDown() end; gl()
    for i = 1, 6 do gf(); dd(); turtle.placeDown() end; gr()
    for i = 1, 3 do gf(); dd(); turtle.placeDown() end; gr()
    for i = 1, 9 do gf(); dd(); turtle.placeDown() end; gr()
    for i = 1, 8 do gf(); dd(); turtle.placeDown() end
    -- Torches
    ss(5); gf(2); gl(); pf(); gu(); gb(10); turtle.placeDown()
    gl(); gf(5); turtle.placeDown(); gf(); turtle.placeDown(); gf(5); turtle.placeDown()
    gr(); gf(11); turtle.placeDown()
    gb(3); gr(); gf(3); turtle.placeDown(); gf(5); turtle.placeDown(); gf(2); gr(); gb(2); gd()

    print("Setup complete!")
    cfg.home_set = true
    cfg.running = true
    save_cfg()
    return true
end

-- ========================================
-- Inventory Check
-- ========================================
local function is_inventory_full()
    local free = 0
    for i = 3, 14 do
        if turtle.getItemCount(i) == 0 then
            free = free + 1
            if free >= INV_FULL_THRESHOLD then return false end
        end
    end
    return true
end

-- ========================================
-- One Farm Round
-- ========================================
local function do_one_round()
    current_state = "farming"
    last_activity = os.clock()

    -- Organize inventory before starting (consolidate saplings/wood)
    organize_inventory()

    -- Go up to patrol level (retry if blocked by another turtle)
    for attempt = 1, 3 do
        if try_up() then break end
        sleep(1 + math.random() * 3)
    end
    ss(SLOT_SAPLING)
    round_counter = round_counter + 1
    local sap_count = turtle.getItemCount(SLOT_SAPLING)
    print("Round " .. round_counter .. " (" .. sap_count .. " saplings)")

    local path_next = PATH_STR:sub(1, 1)
    local inv_full = false
    local steps_since_comms = 0

    for i = 1, LOOP_END do
        local path_now = path_next
        if i < LOOP_END then
            path_next = PATH_STR:sub(i + 1, i + 1)
        else
            path_next = PATH_STR:sub(1, 1)
        end

        -- Move one step
        path_step(i)
        last_activity = os.clock()
        steps_since_comms = steps_since_comms + 1

        -- Mid-patrol comms: send heartbeat + check for commands every N steps
        -- path_comms handles descending to modem level itself
        if steps_since_comms >= PATH_COMMS_INTERVAL then
            path_comms(i)
            steps_since_comms = 0
            -- Abort round if stop command received mid-patrol
            if not farm_running then
                print("  Stop received mid-round — heading home")
                break
            end
        end

        -- Skip tree cutting if inventory is nearly full — just walk home
        if not inv_full then
            local has_saplings = turtle.getItemCount(SLOT_SAPLING) > 1
                and is_sapling_item(SLOT_SAPLING)

            -- Check left side for tree (inspect to avoid mistaking other turtles)
            if path_now ~= "a" and path_next ~= "a" then
                gl()
                if is_tree_block() then
                    cut_tree()
                    -- After cutting, turtle is back at patrol height
                    -- Good time for a comms check (cut_tree ends with try_up)
                    if steps_since_comms >= PATH_COMMS_INTERVAL then
                        path_comms(i)
                        steps_since_comms = 0
                    end
                elseif not turtle.detect() then
                    -- Empty spot — drop to ground, sweep items, try plant
                    if turtle.down() then
                        sf(); turtle.suckDown()
                        if has_saplings then
                            ss(SLOT_SAPLING)
                            turtle.place()
                            ss(1)
                        end
                        -- At ground level (home+0) — modem is directly below, piggyback comms
                        if steps_since_comms >= PATH_COMMS_INTERVAL then
                            path_comms(i, true)
                            steps_since_comms = 0
                        end
                        turtle.up()
                    end
                end
                gr()
            end
            -- Check right side for tree
            if path_now ~= "d" and path_next ~= "d" then
                gr()
                if is_tree_block() then
                    cut_tree()
                    if steps_since_comms >= PATH_COMMS_INTERVAL then
                        path_comms(i)
                        steps_since_comms = 0
                    end
                elseif not turtle.detect() then
                    if turtle.down() then
                        sf(); turtle.suckDown()
                        if has_saplings then
                            ss(SLOT_SAPLING)
                            turtle.place()
                            ss(1)
                        end
                        -- At ground level — piggyback comms
                        if steps_since_comms >= PATH_COMMS_INTERVAL then
                            path_comms(i, true)
                            steps_since_comms = 0
                        end
                        turtle.up()
                    end
                end
                gl()
            end

            -- Check inventory after cutting
            if is_inventory_full() then
                inv_full = true
                print("  Inventory full — heading home to deposit")
            end
        end

        -- Always suck items below (even when full, turtle may have space in partial stacks)
        sd()
    end
end

-- ========================================
-- Main Farm Loop
-- ========================================
local function farm_loop()
    -- Initial position: home = above chest (home+0)
    -- do_one_round() handles going up to patrol height itself

    -- Stagger start based on computer ID so multiple turtles on
    -- the same farm spread out naturally instead of clumping
    local stagger = (os.getComputerID() % 4) * 5  -- 0, 5, 10, or 15s
    if stagger > 0 then
        print("Stagger delay: " .. stagger .. "s")
        sleep(stagger)
    end

    while true do
        last_activity = os.clock()

        -- Organize inventory so wood is in slot 2 for fuel crafting
        organize_inventory()

        -- Craft fuel BEFORE emptying (needs wood in slot 2 + chest in slot 16)
        if not craft_fuel() then
            check_refuel()
        end

        -- Empty into chest (only drops into confirmed container, never into turtles)
        empty_turtle()

        -- Communicate with Wraith (modem is adjacent at chest level)
        do_comms()

        -- Check if we should stop
        if not farm_running then
            current_state = "idle"
            print("Farming paused. Waiting for start command...")
            -- Stay at home, poll modem periodically
            while not farm_running do
                sleep(10)
                do_comms()
            end
        end

        -- Check loop count
        if cfg.loop_count > 0 and round_counter >= cfg.loop_count then
            current_state = "idle"
            farm_running = false
            print("Completed " .. cfg.loop_count .. " rounds.")
            -- Keep checking comms for restart command
            while not farm_running do
                sleep(10)
                do_comms()
            end
            round_counter = 0
        end

        -- Check fuel is sufficient
        if not check_refuel() then
            current_state = "stuck"
            print("Low fuel! Waiting...")
            for attempt = 1, 30 do
                sleep(10)
                do_comms()
                ss(SLOT_REFUEL)
                turtle.refuel()
                ss(1)
                if turtle.getFuelLevel() >= MIN_FUEL then break end
                if craft_fuel() then break end
            end
            if turtle.getFuelLevel() < 100 then
                print("Critically low fuel! Idling.")
                while turtle.getFuelLevel() < 100 do
                    sleep(30)
                    do_comms()
                    ss(SLOT_REFUEL); turtle.refuel(); ss(1)
                end
            end
        end

        -- Do one farming round (goes up to patrol, walks loop, ends at patrol height)
        local ok, err = pcall(do_one_round)
        if ok then
            -- Return from patrol height (home+1) to home level (home+0)
            turtle.down()
        else
            print("Round error: " .. tostring(err):sub(1, 40))
            current_state = "stuck"
            -- Try to recover: go down until we can't (should reach home/chest level)
            for i = 1, 50 do
                if not turtle.down() then
                    if not is_protected(turtle.inspectDown) then turtle.digDown() end
                    if not turtle.down() then break end
                end
            end
            sleep(2)
        end
    end
end

-- ========================================
-- Entry Point
-- ========================================
if not os.getComputerLabel() then
    os.setComputerLabel("Wraith TreeFarm " .. os.getComputerID())
end

load_cfg()

print("Wraith Tree Farm Client v" .. VERSION)
print("ID: " .. os.getComputerID())
print("Label: " .. (os.getComputerLabel() or "none"))
print("Fuel: " .. turtle.getFuelLevel())
print()

-- Handle args
local tArgs = {...}
local do_setup = false
for _, arg in ipairs(tArgs) do
    if arg == "setup" or arg == "set-up" then
        do_setup = true
    end
end

-- Setup mode
if do_setup then
    if run_setup() then
        print("Farm built! Reboot to start farming.")
    end
    return
end

-- Auto-set home on first run
if not cfg.home_set then
    print("First run — setting home position here")
    cfg.home_set = true
    cfg.running = true
    save_cfg()
end

-- Position correction: if we rebooted while on the modem (e.g. after update),
-- step back to home position over the chest
if is_modem_block(turtle.inspectDown) then
    print("On modem — stepping back to home")
    for i = 1, MAX_MOVE_TRIES do
        if turtle.back() then break end
        sleep(0.3)
    end
end

-- Auto-start farming
farm_running = cfg.running
if farm_running then
    print("Auto-starting farm loop...")
else
    print("Farm configured but paused (send start command)")
end

-- Initial comms
do_comms()

-- Run farm loop (with top-level error recovery)
while true do
    local ok, err = pcall(farm_loop)
    if not ok then
        print("FATAL: " .. tostring(err):sub(1, 50))
        print("Recovering in 10s...")
        current_state = "stuck"
        -- Try to check for updates even when crashing (so a fix can be pushed)
        pcall(do_comms)
        sleep(10)
        -- Try to get back to home (descend to chest level)
        for i = 1, 60 do
            if not turtle.down() then
                if not is_protected(turtle.inspectDown) then turtle.digDown() end
                if not turtle.down() then break end
            end
        end
    end
end
