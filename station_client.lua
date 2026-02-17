-- =============================================
-- WRAITH OS - STATION CLIENT
-- =============================================
-- Run on wireless modem PCs at each train station.
-- Controls powered rail (brake/dispatch), track switches
-- via redstone integrator, monitors for route display,
-- and syncs with Wraith OS.
--
-- Setup: Place computer at station with wireless modem.
--        Attach monitor for route map display.
--        Configure rail_side for powered rail redstone.
--        Optional: attach redstoneIntegrator for switches.
--        Run: station_client

local CLIENT_TYPE = "station_client"

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
    ping      = "wraith_rail_st_ping",
    status    = "wraith_rail_st_status",
    register  = "wraith_rail_st_register",
    command   = "wraith_rail_st_cmd",
    heartbeat = "wraith_rail_st_hb",
}
local UPDATE_PROTO = {
    ping = "wraith_update_ping",
    push = "wraith_update_push",
    ack  = "wraith_update_ack",
}
local HEARTBEAT_INTERVAL = 5
local DISCOVERY_INTERVAL = 10
local DISCOVERY_TIMEOUT = 3
local DISPATCH_PULSE = 1.5   -- seconds to power rail for dispatch

-- ========================================
-- Config Persistence
-- ========================================
local CONFIG_FILE = "station_config.lua"

local station_config = {
    label = nil,               -- station name
    rail_side = "back",        -- redstone side for powered rail
    switches = {},             -- {{peripheral_name, face, description, routes}, ...}
    storage_bays = {},         -- {{switch_idx, description}, ...}
    rules = {},                -- automation rules
}

local function load_config()
    if fs.exists(CONFIG_FILE) then
        local f = fs.open(CONFIG_FILE, "r")
        if f then
            local data = f.readAll()
            f.close()
            local fn = loadstring("return " .. data)
            if fn then
                local ok, saved = pcall(fn)
                if ok and type(saved) == "table" then
                    for k, v in pairs(saved) do
                        station_config[k] = v
                    end
                end
            end
        end
    end
end

local function save_config()
    local f = fs.open(CONFIG_FILE, "w")
    if f then
        f.write(textutils.serialise(station_config))
        f.close()
    end
end

load_config()

-- Default label
if not station_config.label then
    station_config.label = "Station " .. os.getComputerID()
    save_config()
end

-- ========================================
-- Modem Setup
-- ========================================
local function find_modem()
    for _, side in ipairs({"back", "top", "left", "right", "bottom", "front"}) do
        if peripheral.getType(side) == "modem" then
            local m = peripheral.wrap(side)
            if m and m.isWireless and m.isWireless() then
                return side
            end
        end
    end
    for _, side in ipairs({"back", "top", "left", "right", "bottom", "front"}) do
        if peripheral.getType(side) == "modem" then return side end
    end
    return nil
end

local modem_side = find_modem()
if not modem_side then
    printError("No modem found! Attach a wireless modem.")
    return
end
rednet.open(modem_side)

-- ========================================
-- GPS Position
-- ========================================
local my_x, my_y, my_z = 0, 0, 0
if gps.locate then
    print("Locating via GPS...")
    local x, y, z = gps.locate(5)
    if x then
        my_x, my_y, my_z = math.floor(x), math.floor(y), math.floor(z)
        print(string.format("GPS: %d, %d, %d", my_x, my_y, my_z))
    else
        print("WARNING: GPS unavailable, position 0,0,0")
    end
end

-- ========================================
-- Peripheral Detection
-- ========================================

-- Monitor (optional, for route display)
local monitor = peripheral.find("monitor")
if monitor then
    monitor.setTextScale(0.5)
    print("Monitor found")
else
    print("No monitor (route display disabled)")
end

-- Redstone Integrators (Advanced Peripherals) for track switches
local redstone_integrators = {}
for _, name in ipairs(peripheral.getNames()) do
    local ptype = peripheral.getType(name)
    if ptype == "redstoneIntegrator" then
        table.insert(redstone_integrators, {
            name = name,
            periph = peripheral.wrap(name),
        })
    end
end
print("Redstone integrators: " .. #redstone_integrators)

-- ========================================
-- Powered Rail Control
-- ========================================
-- Default: unpowered (brake). Power briefly to dispatch.

local has_train = false   -- assume no train until detected

local function brake_on()
    -- Ensure rail is unpowered (stops incoming trains)
    rs.setOutput(station_config.rail_side, false)
end

local function dispatch()
    -- Power the rail to launch train, then return to brake
    print("Dispatching: powering rail on " .. station_config.rail_side)
    rs.setOutput(station_config.rail_side, true)
    os.sleep(DISPATCH_PULSE)
    rs.setOutput(station_config.rail_side, false)
    has_train = false
    print("Dispatch complete, rail braked")
end

-- Start with brake on
brake_on()

-- ========================================
-- Track Switch Control
-- ========================================

local function set_switch(switch_idx, state_on)
    local sw = station_config.switches[switch_idx]
    if not sw then return false end

    -- Find the redstone integrator for this switch
    local periph = nil
    for _, ri in ipairs(redstone_integrators) do
        if ri.name == sw.peripheral_name then
            periph = ri.periph
            break
        end
    end

    if not periph then
        -- Fallback: try direct redstone if face matches a side
        local valid_sides = {top=true, bottom=true, left=true, right=true, front=true, back=true}
        if valid_sides[sw.face] then
            rs.setOutput(sw.face, state_on)
            sw.state = state_on
            save_config()
            return true
        end
        return false
    end

    -- Use redstone integrator
    local ok = pcall(periph.setOutput, sw.face, state_on)
    if ok then
        sw.state = state_on
        save_config()
        print(string.format("Switch %d [%s:%s] -> %s",
            switch_idx, sw.peripheral_name, sw.face, state_on and "ON" or "OFF"))
    end
    return ok
end

-- ========================================
-- Monitor Display
-- ========================================
local route_data = nil   -- received from Wraith

local function render_monitor()
    if not monitor then return end

    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    -- Title
    monitor.setCursorPos(1, 1)
    monitor.setTextColor(colors.cyan)
    monitor.write(station_config.label)

    -- Status
    monitor.setCursorPos(1, 2)
    if WRAITH_ID then
        monitor.setTextColor(colors.lime)
        monitor.write("Connected to Wraith")
    else
        monitor.setTextColor(colors.red)
        monitor.write("Disconnected")
    end

    -- Train status
    monitor.setCursorPos(1, 3)
    monitor.setTextColor(colors.white)
    monitor.write("Train: ")
    if has_train then
        monitor.setTextColor(colors.lime)
        monitor.write("READY")
    else
        monitor.setTextColor(colors.gray)
        monitor.write("NONE")
    end

    -- Position
    monitor.setCursorPos(1, 4)
    monitor.setTextColor(colors.gray)
    monitor.write(string.format("Pos: %d, %d, %d", my_x, my_y, my_z))

    -- Separator
    monitor.setCursorPos(1, 5)
    monitor.setTextColor(colors.gray)
    monitor.write(string.rep("-", mw))

    -- Destination buttons
    monitor.setCursorPos(1, 6)
    monitor.setTextColor(colors.cyan)
    monitor.write("DESTINATIONS:")

    local btn_y = 7
    local dest_buttons = {}

    if route_data and route_data.stations then
        for id, st in pairs(route_data.stations) do
            if id ~= os.getComputerID() and btn_y <= mh then
                local lbl = st.label or ("Station #" .. id)
                if st.is_hub then lbl = "\4 " .. lbl end

                monitor.setCursorPos(2, btn_y)
                if has_train then
                    monitor.setBackgroundColor(colors.blue)
                    monitor.setTextColor(colors.white)
                else
                    monitor.setBackgroundColor(colors.gray)
                    monitor.setTextColor(colors.lightGray)
                end
                local btn_text = " " .. lbl:sub(1, mw - 4) .. string.rep(" ", math.max(0, mw - 4 - #lbl))
                monitor.write(btn_text)
                monitor.setBackgroundColor(colors.black)

                table.insert(dest_buttons, {y = btn_y, id = id, label = lbl})
                btn_y = btn_y + 2
            end
        end
    else
        monitor.setCursorPos(2, btn_y)
        monitor.setTextColor(colors.gray)
        monitor.write("No route data")
    end

    -- Switch status at bottom
    if #station_config.switches > 0 then
        local sy = mh - #station_config.switches
        if sy < btn_y + 1 then sy = btn_y + 1 end
        monitor.setCursorPos(1, sy)
        monitor.setTextColor(colors.cyan)
        monitor.write("SWITCHES:")
        for si, sw in ipairs(station_config.switches) do
            if sy + si <= mh then
                monitor.setCursorPos(2, sy + si)
                monitor.setTextColor(sw.state and colors.lime or colors.gray)
                monitor.write(string.format("%d. %s [%s]",
                    si, sw.description or sw.face, sw.state and "ON" or "OFF"))
            end
        end
    end

    return dest_buttons
end

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
-- Discovery & Registration
-- ========================================
local function discover_wraith()
    print("Searching for Wraith OS...")
    rednet.broadcast({
        type = "station",
        label = station_config.label,
        id = os.getComputerID(),
        x = my_x, y = my_y, z = my_z,
        rail_side = station_config.rail_side,
        switches = station_config.switches,
        storage_bays = station_config.storage_bays,
        has_train = has_train,
    }, PROTOCOLS.ping)

    local sender, msg = rednet.receive(PROTOCOLS.status, DISCOVERY_TIMEOUT)
    if sender and type(msg) == "table" and msg.status == "wraith_rail_hub" then
        WRAITH_ID = sender
        if msg.stations then
            route_data = msg.stations
        end
        print("Found Wraith OS at #" .. sender)
        return true
    end
    return false
end

local function register_with_wraith()
    if not WRAITH_ID then return false end
    rednet.send(WRAITH_ID, {
        label = station_config.label,
        x = my_x, y = my_y, z = my_z,
        rail_side = station_config.rail_side,
        switches = station_config.switches,
        storage_bays = station_config.storage_bays,
        rules = station_config.rules,
        has_train = has_train,
    }, PROTOCOLS.register)
    local sender, msg = rednet.receive(PROTOCOLS.status, 3)
    if sender == WRAITH_ID and type(msg) == "table" and msg.status == "registered" then
        print("Registered with Wraith #" .. WRAITH_ID)
        return true
    end
    return false
end

-- ========================================
-- Discovery Loop
-- ========================================
while not WRAITH_ID do
    if not discover_wraith() then
        print("Wraith not found, retrying in " .. DISCOVERY_INTERVAL .. "s...")
        os.sleep(DISCOVERY_INTERVAL)
    end
end

-- Registration
local registered = false
for attempt = 1, 5 do
    if register_with_wraith() then
        registered = true
        break
    end
    os.sleep(2)
end
if not registered then
    print("WARNING: Could not confirm registration")
end

-- ========================================
-- Display status
-- ========================================
term.clear()
term.setCursorPos(1, 1)
print("=== Station Client v" .. VERSION .. " ===")
print("Computer #" .. os.getComputerID())
print("Station:   " .. station_config.label)
print("Wraith:    #" .. tostring(WRAITH_ID))
print("Position:  " .. my_x .. ", " .. my_y .. ", " .. my_z)
print("Rail side: " .. station_config.rail_side)
print("Switches:  " .. #station_config.switches)
print("Monitor:   " .. (monitor and "YES" or "NO"))
print("")
print("Listening for commands...")
print("")

-- ========================================
-- Main Loops
-- ========================================

local dest_buttons = {}

local function command_listener()
    while true do
        local sender, msg, proto = rednet.receive(nil, 1)

        if sender and proto == PROTOCOLS.command and type(msg) == "table" then
            if msg.action == "dispatch" then
                -- Dispatch command from Wraith
                print(string.format("DISPATCH to %s", msg.destination_label or "?"))
                dispatch()

            elseif msg.action == "set_switch" then
                -- Switch control from Wraith
                local sw_idx = msg.switch_idx
                local sw_state = msg.state
                if sw_idx then
                    set_switch(sw_idx, sw_state)
                end

            elseif msg.action == "brake" then
                -- Emergency brake
                brake_on()

            elseif msg.action == "set_rail_side" then
                -- Configure rail side
                station_config.rail_side = msg.side or "back"
                save_config()
                brake_on()
                print("Rail side set to: " .. station_config.rail_side)

            elseif msg.action == "set_label" then
                station_config.label = msg.label
                save_config()
                print("Label set to: " .. station_config.label)

            elseif msg.action == "add_switch" then
                table.insert(station_config.switches, {
                    peripheral_name = msg.peripheral_name,
                    face = msg.face or "top",
                    description = msg.description or "Switch",
                    state = false,
                    routes = msg.routes or {},
                })
                save_config()
                print("Switch added: " .. (msg.description or "Switch"))

            elseif msg.action == "remove_switch" then
                if station_config.switches[msg.idx] then
                    table.remove(station_config.switches, msg.idx)
                    save_config()
                    print("Switch removed: #" .. msg.idx)
                end

            elseif msg.action == "update_routes" then
                -- Update route map data for monitor display
                if msg.stations then
                    route_data = msg
                end

            elseif msg.action == "set_has_train" then
                has_train = msg.value or false

            elseif msg.action == "add_rule" then
                table.insert(station_config.rules, msg.rule)
                save_config()

            elseif msg.action == "remove_rule" then
                if station_config.rules[msg.idx] then
                    table.remove(station_config.rules, msg.idx)
                    save_config()
                end
            end

            -- Send updated status back
            if WRAITH_ID then
                rednet.send(WRAITH_ID, {
                    rules = station_config.rules,
                    switches = station_config.switches,
                    storage_bays = station_config.storage_bays,
                    rail_side = station_config.rail_side,
                    has_train = has_train,
                }, PROTOCOLS.status)
            end

        elseif sender and proto == PROTOCOLS.status then
            -- Status response (e.g. route updates)
            if type(msg) == "table" and msg.stations then
                route_data = msg.stations
            end
        end
    end
end

local function heartbeat_sender()
    while true do
        sleep(HEARTBEAT_INTERVAL)
        if WRAITH_ID then
            rednet.send(WRAITH_ID, {
                id = os.getComputerID(),
                label = station_config.label,
                has_train = has_train,
            }, PROTOCOLS.heartbeat)
        end
    end
end

local function discovery_loop()
    local missed_pings = 0
    while true do
        sleep(60)
        if WRAITH_ID then
            rednet.send(WRAITH_ID, {
                type = "station",
                label = station_config.label,
                id = os.getComputerID(),
                x = my_x, y = my_y, z = my_z,
                has_train = has_train,
            }, PROTOCOLS.ping)
            local _, resp = rednet.receive(PROTOCOLS.status, 5)
            if not resp then
                missed_pings = missed_pings + 1
                if missed_pings >= 3 then
                    print("Lost connection (3 missed). Rediscovering...")
                    WRAITH_ID = nil
                    missed_pings = 0
                    while not WRAITH_ID do
                        if discover_wraith() then break end
                        sleep(DISCOVERY_INTERVAL)
                    end
                end
            else
                missed_pings = 0
                if type(resp) == "table" and resp.stations then
                    route_data = resp.stations
                end
            end
        end
    end
end

local function monitor_loop()
    while true do
        dest_buttons = render_monitor() or {}
        os.sleep(1)
    end
end

local function monitor_touch_loop()
    if not monitor then
        while true do sleep(60) end
    end
    while true do
        local ev, side, tx, ty = os.pullEvent("monitor_touch")
        -- Check destination button presses
        for _, btn in ipairs(dest_buttons) do
            if ty == btn.y and has_train then
                print("Destination selected: " .. btn.label)
                -- Request dispatch via Wraith
                if WRAITH_ID then
                    rednet.send(WRAITH_ID, {
                        action = "request_dispatch",
                        from = os.getComputerID(),
                        to = btn.id,
                    }, PROTOCOLS.status)
                end
                -- Also dispatch locally
                dispatch()
                break
            end
        end
    end
end

local function update_checker()
    while true do
        sleep(300)
        check_for_updates()
    end
end

parallel.waitForAll(
    command_listener,
    heartbeat_sender,
    discovery_loop,
    monitor_loop,
    monitor_touch_loop,
    update_checker
)
