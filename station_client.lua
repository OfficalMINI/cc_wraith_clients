-- =============================================
-- WRAITH OS - STATION CLIENT
-- =============================================
-- Run on wireless modem PCs at each train station.
-- All redstone I/O (powered rail, detector rail, switches)
-- uses Advanced Peripherals redstone integrators
-- connected via wired modem.
--
-- Setup: Place computer at station with wireless + wired modem.
--        Connect redstone integrators via wired modem network.
--        Attach monitor for route map display.
--        Configure rail integrator + face for powered rail output.
--        Configure detector integrator + face for detector rail input.
--        Run: station_client

local CLIENT_TYPE = "station_client"

-- Compute version hash from own file content
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
local UPDATE_URL = "https://raw.githubusercontent.com/OfficalMINI/cc_wraith_clients/refs/heads/main/station_client.lua"
local WRAITH_ID = nil

local PROTOCOLS = {
    ping      = "wraith_rail_st_ping",
    status    = "wraith_rail_st_status",
    register  = "wraith_rail_st_register",
    command   = "wraith_rail_st_cmd",
    heartbeat = "wraith_rail_st_hb",
}
local HEARTBEAT_INTERVAL = 5
local DISCOVERY_INTERVAL = 10
local DISCOVERY_TIMEOUT = 3
local DISPATCH_PULSE = 1.5       -- seconds to power rail for dispatch
local DETECTOR_FALLBACK_POLL = 0.1 -- poll interval (~2 game ticks, fastest practical)

-- ========================================
-- Config Persistence
-- ========================================
local CONFIG_FILE = "station_config.lua"

local station_config = {
    label = nil,               -- station name
    rail_periph = nil,         -- peripheral name of redstone integrator for powered rail
    rail_face = "top",          -- face on the integrator for powered rail output
    detector_periph = nil,     -- peripheral name of redstone integrator for detector rail
    detector_face = "top",      -- face on the integrator for detector rail input
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

-- Redstone Integrators (Advanced Peripherals) via wired modem
local redstone_integrators = {}
local function scan_integrators()
    redstone_integrators = {}
    for _, name in ipairs(peripheral.getNames()) do
        local ptype = peripheral.getType(name)
        if ptype == "redstoneIntegrator" then
            table.insert(redstone_integrators, {
                name = name,
                periph = peripheral.wrap(name),
            })
        end
    end
end
scan_integrators()
print("Redstone integrators: " .. #redstone_integrators)

-- Helper: find integrator by peripheral name
local function get_integrator(periph_name)
    if not periph_name then return nil end
    for _, ri in ipairs(redstone_integrators) do
        if ri.name == periph_name then
            return ri.periph
        end
    end
    return nil
end

-- ========================================
-- Interactive Setup
-- ========================================
-- Runs on first boot or when peripherals aren't configured.
-- Also accessible by running: station_client setup

local FACES = {"top", "bottom", "north", "south", "east", "west"}

local function pick_number(prompt, max)
    while true do
        write(prompt)
        local input = read()
        local n = tonumber(input)
        if n and n >= 1 and n <= max then return n end
        print("Enter 1-" .. max)
    end
end

local function pick_integrator(purpose)
    print("")
    print("== Select integrator for: " .. purpose .. " ==")
    for i, ri in ipairs(redstone_integrators) do
        print(string.format("  %d. %s", i, ri.name))
    end
    local idx = pick_number("Choice [1-" .. #redstone_integrators .. "]: ", #redstone_integrators)
    return redstone_integrators[idx].name
end

local function pick_face(purpose, default)
    print("")
    print("== Select face for: " .. purpose .. " ==")
    for i, f in ipairs(FACES) do
        local mark = (f == default) and " (default)" or ""
        print(string.format("  %d. %s%s", i, f, mark))
    end
    write("Choice [1-6, Enter=" .. default .. "]: ")
    local input = read()
    if input == "" then return default end
    local n = tonumber(input)
    if n and n >= 1 and n <= 6 then return FACES[n] end
    return default
end

local function run_setup()
    term.clear()
    term.setCursorPos(1, 1)
    print("========================================")
    print("  STATION SETUP - " .. station_config.label)
    print("========================================")
    print("")

    scan_integrators()

    if #redstone_integrators == 0 then
        print("ERROR: No redstone integrators found!")
        print("Connect integrators via wired modem and restart.")
        print("")
        print("Press any key to continue without setup...")
        os.pullEvent("key")
        return
    end

    print("Found " .. #redstone_integrators .. " integrator(s):")
    for i, ri in ipairs(redstone_integrators) do
        print(string.format("  %d. %s", i, ri.name))
    end

    -- Station label
    print("")
    write("Station name [" .. station_config.label .. "]: ")
    local new_label = read()
    if new_label ~= "" then
        station_config.label = new_label
    end

    -- Powered rail integrator
    station_config.rail_periph = pick_integrator("POWERED RAIL (output)")
    station_config.rail_face = pick_face("powered rail face", station_config.rail_face)

    -- Detector rail integrator
    station_config.detector_periph = pick_integrator("DETECTOR RAIL (input)")
    station_config.detector_face = pick_face("detector rail face", station_config.detector_face)

    -- Track switches
    print("")
    write("Configure track switches? [y/N]: ")
    local sw_ans = read()
    if sw_ans == "y" or sw_ans == "Y" then
        while true do
            print("")
            local sw_periph = pick_integrator("SWITCH #" .. (#station_config.switches + 1))
            local sw_face = pick_face("switch output face", "top")
            write("Switch description: ")
            local sw_desc = read()
            if sw_desc == "" then sw_desc = "Switch " .. (#station_config.switches + 1) end
            table.insert(station_config.switches, {
                peripheral_name = sw_periph,
                face = sw_face,
                description = sw_desc,
                state = false,
                routes = {},
            })
            print("Added: " .. sw_desc .. " [" .. sw_periph .. ":" .. sw_face .. "]")
            write("Add another switch? [y/N]: ")
            local more = read()
            if more ~= "y" and more ~= "Y" then break end
        end
    end

    save_config()

    print("")
    print("========================================")
    print("  Setup complete! Config saved.")
    print("========================================")
    print("  Rail:     " .. station_config.rail_periph .. ":" .. station_config.rail_face)
    print("  Detector: " .. station_config.detector_periph .. ":" .. station_config.detector_face)
    print("  Switches: " .. #station_config.switches)
    print("========================================")
    sleep(1)
end

-- Run setup if unconfigured or requested via command line arg
local args = {...}
if args[1] == "setup" then
    run_setup()
elseif not station_config.rail_periph or not station_config.detector_periph then
    if #redstone_integrators > 0 then
        run_setup()
    else
        print("WARNING: No integrators found and none configured!")
        print("Run 'station_client setup' after connecting peripherals.")
    end
end

-- ========================================
-- Powered Rail Control (via integrator)
-- ========================================
-- Default: unpowered (brake). Power briefly to dispatch.

local has_train = false   -- detected by detector rail integrator

local function brake_on()
    local ri = get_integrator(station_config.rail_periph)
    if ri then
        pcall(ri.setOutput, station_config.rail_face, false)
    end
end

local function dispatch()
    local ri = get_integrator(station_config.rail_periph)
    if not ri then
        print("ERROR: Rail integrator not found: " .. tostring(station_config.rail_periph))
        return
    end
    print(string.format("Dispatching: %s:%s -> ON",
        station_config.rail_periph, station_config.rail_face))
    pcall(ri.setOutput, station_config.rail_face, true)
    os.sleep(DISPATCH_PULSE)
    pcall(ri.setOutput, station_config.rail_face, false)
    has_train = false
    print("Dispatch complete, rail braked")
end

-- Start with brake on
brake_on()

-- ========================================
-- Detector Rail Monitoring (via integrator)
-- ========================================
-- Detector rails output redstone when a minecart sits on them.
-- AP redstone integrators fire "redstoneIntegrator" events (0.8+)
-- when input changes: event, side, peripheral_name.
-- Slow fallback poll for older AP versions.

local detector_debug = true  -- verbose logging for every read

local function check_detector()
    if not station_config.detector_periph then
        print("[det] NO PERIPH CONFIGURED")
        return false
    end

    local ri = get_integrator(station_config.detector_periph)
    if not ri then
        print("[det] PERIPH NOT FOUND: " .. station_config.detector_periph)
        return false
    end

    -- Read boolean input
    local ok, signal = pcall(ri.getInput, station_config.detector_face)
    -- Also read analog for extra info
    local ok2, analog = pcall(ri.getAnalogInput, station_config.detector_face)

    if not ok then
        print("[det] getInput ERROR: " .. tostring(signal))
        return false
    end

    if detector_debug then
        -- Show ALL faces to find where signal actually is
        local parts = {}
        for _, f in ipairs({"top","bottom","north","south","east","west"}) do
            local fok, fval = pcall(ri.getInput, f)
            local fok2, fana = pcall(ri.getAnalogInput, f)
            if (fok and fval) or (fok2 and fana and fana > 0) then
                table.insert(parts, f .. "=" .. tostring(fana) .. "***")
            else
                table.insert(parts, f .. "=" .. tostring(fana or 0))
            end
        end
        print("[det] " .. table.concat(parts, " "))
    end

    if signal and not has_train then
        has_train = true
        print("[det] >>> TRAIN ARRIVED <<<")
    elseif not signal and has_train then
        has_train = false
        print("[det] >>> TRAIN DEPARTED <<<")
    end
    return true
end

-- Initial diagnostics
print("[det] === DETECTOR DIAGNOSTICS ===")
print("[det] Configured periph: " .. tostring(station_config.detector_periph))
print("[det] Configured face:   " .. tostring(station_config.detector_face))
print("[det] Integrators on network: " .. #redstone_integrators)
for _, ri in ipairs(redstone_integrators) do
    print("[det] --- " .. ri.name .. " ---")
    for _, face in ipairs({"top","bottom","north","south","east","west"}) do
        local ok1, dig = pcall(ri.periph.getInput, face)
        local ok2, ana = pcall(ri.periph.getAnalogInput, face)
        local tag = ""
        if (ok1 and dig) or (ok2 and ana and ana > 0) then tag = " ***" end
        print(string.format("[det]   %s: d=%s a=%s%s",
            face,
            ok1 and tostring(dig) or "ERR:"..tostring(dig),
            ok2 and tostring(ana) or "ERR:"..tostring(ana),
            tag))
    end
end
print("[det] ===========================")
check_detector()

-- ========================================
-- Track Switch Control (via integrator)
-- ========================================

local function set_switch(switch_idx, state_on)
    local sw = station_config.switches[switch_idx]
    if not sw then return false end

    local periph = get_integrator(sw.peripheral_name)
    if not periph then
        print(string.format("Switch %d: integrator '%s' not found",
            switch_idx, tostring(sw.peripheral_name)))
        return false
    end

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

-- Monitor UI state
local monitor_mode = "main"  -- "main", "config", "pick_integrator", "pick_face"
local config_purpose = nil   -- "rail" or "detector" (what we're currently configuring)
local monitor_buttons = {}   -- rebuilt each render: {{y1, y2, action, data}, ...}

local function mon_btn(y1, y2, action, data)
    table.insert(monitor_buttons, {y1 = y1, y2 = y2 or y1, action = action, data = data})
end

local function render_main_monitor()
    if not monitor then return end

    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    -- Title + SETUP button
    monitor.setCursorPos(1, 1)
    monitor.setTextColor(colors.cyan)
    monitor.write(station_config.label)
    -- SETUP button top-right
    local setup_lbl = "[SETUP]"
    monitor.setCursorPos(mw - #setup_lbl + 1, 1)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.white)
    monitor.write(setup_lbl)
    monitor.setBackgroundColor(colors.black)
    mon_btn(1, 1, "open_config", {x1 = mw - #setup_lbl + 1, x2 = mw})

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
        monitor.write("DETECTED")
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

                mon_btn(btn_y, btn_y, "dispatch_to", {id = id, label = lbl})
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
end

local function render_config_monitor()
    if not monitor then return end

    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    -- Back button
    monitor.setCursorPos(1, 1)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.white)
    monitor.write("< BACK")
    monitor.setBackgroundColor(colors.black)
    mon_btn(1, 1, "close_config", {x1 = 1, x2 = 6})

    -- Title
    monitor.setCursorPos(1, 2)
    monitor.setTextColor(colors.cyan)
    monitor.write("STATION SETUP")

    -- Separator
    monitor.setCursorPos(1, 3)
    monitor.setTextColor(colors.gray)
    monitor.write(string.rep("-", mw))

    local cy = 4

    -- Rail integrator
    monitor.setCursorPos(1, cy)
    monitor.setTextColor(colors.white)
    monitor.write("Powered Rail:")
    cy = cy + 1
    monitor.setCursorPos(2, cy)
    monitor.setTextColor(colors.yellow)
    local rail_name = station_config.rail_periph or "NOT SET"
    local rail_lbl = rail_name:sub(1, mw - 10) .. ":" .. (station_config.rail_face or "?")
    monitor.write(rail_lbl)
    monitor.setCursorPos(mw - 7, cy)
    monitor.setBackgroundColor(colors.blue)
    monitor.setTextColor(colors.white)
    monitor.write("[CHANGE]")
    monitor.setBackgroundColor(colors.black)
    mon_btn(cy, cy, "pick_rail", {x1 = mw - 7, x2 = mw})
    cy = cy + 1

    -- Detector integrator
    cy = cy + 1
    monitor.setCursorPos(1, cy)
    monitor.setTextColor(colors.white)
    monitor.write("Detector Rail:")
    cy = cy + 1
    monitor.setCursorPos(2, cy)
    monitor.setTextColor(colors.yellow)
    local det_name = station_config.detector_periph or "NOT SET"
    local det_lbl = det_name:sub(1, mw - 10) .. ":" .. (station_config.detector_face or "?")
    monitor.write(det_lbl)
    monitor.setCursorPos(mw - 7, cy)
    monitor.setBackgroundColor(colors.blue)
    monitor.setTextColor(colors.white)
    monitor.write("[CHANGE]")
    monitor.setBackgroundColor(colors.black)
    mon_btn(cy, cy, "pick_detector", {x1 = mw - 7, x2 = mw})
    cy = cy + 1

    -- Switches summary
    cy = cy + 1
    monitor.setCursorPos(1, cy)
    monitor.setTextColor(colors.gray)
    monitor.write(string.format("Switches: %d configured", #station_config.switches))
    cy = cy + 1

    -- Integrators summary
    cy = cy + 1
    monitor.setCursorPos(1, cy)
    monitor.setTextColor(colors.gray)
    monitor.write(string.format("Integrators found: %d", #redstone_integrators))
    cy = cy + 1

    -- Rescan button
    cy = cy + 1
    if cy <= mh then
        monitor.setCursorPos(2, cy)
        monitor.setBackgroundColor(colors.gray)
        monitor.setTextColor(colors.white)
        local rescan_lbl = " RESCAN PERIPHERALS "
        monitor.write(rescan_lbl)
        monitor.setBackgroundColor(colors.black)
        mon_btn(cy, cy, "rescan", {x1 = 2, x2 = 2 + #rescan_lbl - 1})
    end
end

local function render_integrator_picker()
    if not monitor then return end

    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    -- Back button
    monitor.setCursorPos(1, 1)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.white)
    monitor.write("< BACK")
    monitor.setBackgroundColor(colors.black)
    mon_btn(1, 1, "back_to_config", {x1 = 1, x2 = 6})

    -- Title
    local purpose_lbl = config_purpose == "rail" and "POWERED RAIL" or "DETECTOR RAIL"
    monitor.setCursorPos(1, 2)
    monitor.setTextColor(colors.cyan)
    monitor.write("SELECT FOR: " .. purpose_lbl)

    monitor.setCursorPos(1, 3)
    monitor.setTextColor(colors.gray)
    monitor.write(string.rep("-", mw))

    -- List integrators
    local cy = 4
    if #redstone_integrators == 0 then
        monitor.setCursorPos(2, cy)
        monitor.setTextColor(colors.red)
        monitor.write("No integrators found!")
    else
        for i, ri in ipairs(redstone_integrators) do
            if cy > mh then break end
            local is_current = false
            if config_purpose == "rail" and ri.name == station_config.rail_periph then
                is_current = true
            elseif config_purpose == "detector" and ri.name == station_config.detector_periph then
                is_current = true
            end

            monitor.setCursorPos(1, cy)
            if is_current then
                monitor.setBackgroundColor(colors.blue)
                monitor.setTextColor(colors.white)
            else
                monitor.setBackgroundColor(colors.gray)
                monitor.setTextColor(colors.white)
            end
            local entry = string.format(" %d. %s %s",
                i, ri.name:sub(1, mw - 8), is_current and "*" or " ")
            monitor.write(entry .. string.rep(" ", math.max(0, mw - #entry)))
            monitor.setBackgroundColor(colors.black)
            mon_btn(cy, cy, "select_integrator", {name = ri.name})
            cy = cy + 1
        end
    end
end

local function render_face_picker()
    if not monitor then return end

    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    -- Back button
    monitor.setCursorPos(1, 1)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.white)
    monitor.write("< BACK")
    monitor.setBackgroundColor(colors.black)
    mon_btn(1, 1, "back_to_config", {x1 = 1, x2 = 6})

    -- Title
    local purpose_lbl = config_purpose == "rail" and "POWERED RAIL" or "DETECTOR RAIL"
    monitor.setCursorPos(1, 2)
    monitor.setTextColor(colors.cyan)
    monitor.write("SELECT FACE: " .. purpose_lbl)

    monitor.setCursorPos(1, 3)
    monitor.setTextColor(colors.gray)
    monitor.write(string.rep("-", mw))

    local current_face
    if config_purpose == "rail" then
        current_face = station_config.rail_face
    else
        current_face = station_config.detector_face
    end

    local cy = 4
    for i, face in ipairs(FACES) do
        if cy > mh then break end
        local is_current = (face == current_face)

        monitor.setCursorPos(1, cy)
        if is_current then
            monitor.setBackgroundColor(colors.blue)
            monitor.setTextColor(colors.white)
        else
            monitor.setBackgroundColor(colors.gray)
            monitor.setTextColor(colors.white)
        end
        local entry = string.format(" %d. %s %s", i, face, is_current and "*" or " ")
        monitor.write(entry .. string.rep(" ", math.max(0, mw - #entry)))
        monitor.setBackgroundColor(colors.black)
        mon_btn(cy, cy, "select_face", {face = face})
        cy = cy + 1
    end
end

local function render_monitor()
    if not monitor then return end
    monitor_buttons = {}

    if monitor_mode == "config" then
        render_config_monitor()
    elseif monitor_mode == "pick_integrator" then
        render_integrator_picker()
    elseif monitor_mode == "pick_face" then
        render_face_picker()
    else
        render_main_monitor()
    end
end

-- ========================================
-- Update Check (GitHub HTTP)
-- ========================================
local function check_for_updates()
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
    local sum = 0
    for i = 1, #content do
        sum = (sum * 31 + string.byte(content, i)) % 2147483647
    end
    local remote_ver = tostring(sum)
    if remote_ver == VERSION then
        print("[update] Up to date (ver=" .. VERSION .. ")")
        return false
    end
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

check_for_updates()

-- ========================================
-- Discovery & Registration
-- ========================================
local function discover_wraith()
    print("Searching for Wraith OS...")
    -- Build integrator name list for Wraith
    local integrator_names = {}
    for _, ri in ipairs(redstone_integrators) do
        table.insert(integrator_names, ri.name)
    end

    rednet.broadcast({
        type = "station",
        label = station_config.label,
        id = os.getComputerID(),
        x = my_x, y = my_y, z = my_z,
        rail_periph = station_config.rail_periph,
        rail_face = station_config.rail_face,
        detector_periph = station_config.detector_periph,
        detector_face = station_config.detector_face,
        switches = station_config.switches,
        storage_bays = station_config.storage_bays,
        integrators = integrator_names,
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
    -- Build integrator name list for Wraith
    local integrator_names = {}
    for _, ri in ipairs(redstone_integrators) do
        table.insert(integrator_names, ri.name)
    end

    rednet.send(WRAITH_ID, {
        label = station_config.label,
        x = my_x, y = my_y, z = my_z,
        rail_periph = station_config.rail_periph,
        rail_face = station_config.rail_face,
        detector_periph = station_config.detector_periph,
        detector_face = station_config.detector_face,
        switches = station_config.switches,
        storage_bays = station_config.storage_bays,
        rules = station_config.rules,
        integrators = integrator_names,
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
print("Station:    " .. station_config.label)
print("Wraith:     #" .. tostring(WRAITH_ID))
print("Position:   " .. my_x .. ", " .. my_y .. ", " .. my_z)
print("Rail:       " .. tostring(station_config.rail_periph) .. ":" .. station_config.rail_face)
print("Detector:   " .. tostring(station_config.detector_periph) .. ":" .. station_config.detector_face)
print("Integrators:" .. #redstone_integrators)
print("Switches:   " .. #station_config.switches)
print("Monitor:    " .. (monitor and "YES" or "NO"))
print("")
print("Listening for commands...")
print("")

-- ========================================
-- Main Loops
-- ========================================

local function command_listener()
    while true do
        local sender, msg, proto = rednet.receive(nil, 1)

        if sender and proto == PROTOCOLS.command and type(msg) == "table" then
            if msg.action == "dispatch" then
                print(string.format("DISPATCH to %s", msg.destination_label or "?"))
                dispatch()

            elseif msg.action == "set_switch" then
                local sw_idx = msg.switch_idx
                local sw_state = msg.state
                if sw_idx then
                    set_switch(sw_idx, sw_state)
                end

            elseif msg.action == "brake" then
                brake_on()

            elseif msg.action == "set_rail" then
                -- Configure rail integrator + face
                station_config.rail_periph = msg.peripheral_name or station_config.rail_periph
                station_config.rail_face = msg.face or station_config.rail_face
                save_config()
                brake_on()
                print(string.format("Rail set to: %s:%s",
                    station_config.rail_periph, station_config.rail_face))

            elseif msg.action == "set_detector" then
                -- Configure detector integrator + face
                station_config.detector_periph = msg.peripheral_name or station_config.detector_periph
                station_config.detector_face = msg.face or station_config.detector_face
                save_config()
                print(string.format("Detector set to: %s:%s",
                    station_config.detector_periph, station_config.detector_face))

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
                    rail_periph = station_config.rail_periph,
                    rail_face = station_config.rail_face,
                    detector_periph = station_config.detector_periph,
                    detector_face = station_config.detector_face,
                    has_train = has_train,
                }, PROTOCOLS.status)
            end

        elseif sender and proto == PROTOCOLS.status then
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
        render_monitor()
        os.sleep(1)
    end
end

local function monitor_touch_loop()
    if not monitor then
        while true do sleep(60) end
    end
    while true do
        local ev, side, tx, ty = os.pullEvent("monitor_touch")

        -- Find which button was tapped
        for _, btn in ipairs(monitor_buttons) do
            if ty >= btn.y1 and ty <= btn.y2 then
                -- Some buttons also check x range
                if btn.data and btn.data.x1 then
                    if tx < btn.data.x1 or tx > btn.data.x2 then
                        -- Outside x range, skip
                        goto continue
                    end
                end

                -- Handle action
                if btn.action == "open_config" then
                    monitor_mode = "config"
                    render_monitor()

                elseif btn.action == "close_config" then
                    monitor_mode = "main"
                    render_monitor()

                elseif btn.action == "back_to_config" then
                    monitor_mode = "config"
                    config_purpose = nil
                    render_monitor()

                elseif btn.action == "dispatch_to" then
                    if has_train then
                        print("Destination selected: " .. btn.data.label)
                        if WRAITH_ID then
                            rednet.send(WRAITH_ID, {
                                action = "request_dispatch",
                                from = os.getComputerID(),
                                to = btn.data.id,
                            }, PROTOCOLS.status)
                        end
                        dispatch()
                        render_monitor()
                    end

                elseif btn.action == "pick_rail" then
                    config_purpose = "rail"
                    monitor_mode = "pick_integrator"
                    render_monitor()

                elseif btn.action == "pick_detector" then
                    config_purpose = "detector"
                    monitor_mode = "pick_integrator"
                    render_monitor()

                elseif btn.action == "select_integrator" then
                    if config_purpose == "rail" then
                        station_config.rail_periph = btn.data.name
                    elseif config_purpose == "detector" then
                        station_config.detector_periph = btn.data.name
                    end
                    save_config()
                    -- Now pick face
                    monitor_mode = "pick_face"
                    render_monitor()

                elseif btn.action == "select_face" then
                    if config_purpose == "rail" then
                        station_config.rail_face = btn.data.face
                        brake_on()  -- re-apply brake with new config
                    elseif config_purpose == "detector" then
                        station_config.detector_face = btn.data.face
                    end
                    save_config()
                    print(string.format("Config updated: %s -> %s:%s",
                        config_purpose,
                        config_purpose == "rail" and station_config.rail_periph or station_config.detector_periph,
                        btn.data.face))
                    -- Return to config menu
                    monitor_mode = "config"
                    config_purpose = nil
                    render_monitor()

                elseif btn.action == "rescan" then
                    scan_integrators()
                    print("Rescanned: " .. #redstone_integrators .. " integrators found")
                    render_monitor()
                end

                break
                ::continue::
            end
        end
    end
end

local function detector_loop()
    -- Event-driven: listen for AP "redstoneIntegrator" events (instant).
    -- Fallback: poll every 0.5s for AP <0.8 where events don't exist.
    print("[detector] Loop started (poll=" .. DETECTOR_FALLBACK_POLL .. "s)")
    local poll_timer = os.startTimer(DETECTOR_FALLBACK_POLL)
    local poll_count = 0

    while true do
        local event, p1, p2 = os.pullEvent()

        if event == "redstoneIntegrator" then
            -- p1 = side, p2 = peripheral name
            print("[detector] EVENT: " .. tostring(p1) .. " from " .. tostring(p2))
            if p2 == station_config.detector_periph then
                check_detector()
            end
        elseif event == "redstone" then
            -- Vanilla redstone event (fires if integrator is local)
            check_detector()
        elseif event == "timer" and p1 == poll_timer then
            check_detector()
            poll_timer = os.startTimer(DETECTOR_FALLBACK_POLL)
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
    detector_loop,
    update_checker
)
