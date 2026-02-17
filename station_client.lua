-- =============================================
-- STATION CLIENT (Hub / Remote)
-- =============================================
-- Run on wireless modem PCs at each train station.
-- All redstone I/O (powered rail, detector rail, switches)
-- uses Advanced Peripherals redstone integrators
-- connected via wired modem.
--
-- One station is configured as "hub" and manages all others.
-- Remote stations discover and register with the hub via rednet.
--
-- Setup: Place computer at station with wireless + wired modem.
--        Connect redstone integrators via wired modem network.
--        Attach monitor for route map display.
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
local AUTO_PARK_DELAY = 5        -- seconds after arrival before auto-parking
local PLAYER_DETECT_RANGE = 16   -- blocks range for player detector
local PLAYER_CHECK_INTERVAL = 2  -- seconds between player proximity checks

-- ========================================
-- Config Persistence
-- ========================================
local CONFIG_FILE = "station_config.lua"

local station_config = {
    label = nil,               -- station name
    is_hub = false,            -- true if this is the hub station
    rail_periph = nil,         -- peripheral name of redstone integrator for powered rail
    rail_face = "top",          -- face on the integrator for powered rail output
    detector_periph = nil,     -- peripheral name of redstone integrator for detector rail
    detector_face = "top",      -- face on the integrator for detector rail input
    switches = {},             -- {{peripheral_name, face, description, routes, parking, bay_*}, ...}
    player_detector = nil,     -- peripheral name of player detector block
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
-- Hub State (only used if is_hub)
-- ========================================
-- Connected remote stations, keyed by computer ID
local connected_stations = {}
-- Hub ID: known hub computer ID (self if hub, discovered if remote)
local HUB_ID = nil

if station_config.is_hub then
    HUB_ID = os.getComputerID()
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
-- Wired modem peripherals may not be ready immediately on boot
if #redstone_integrators == 0 then
    os.sleep(0.5)
    scan_integrators()
end
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

-- Player Detectors (Advanced Peripherals) via wired modem
local player_detectors = {}
local function scan_player_detectors()
    player_detectors = {}
    for _, name in ipairs(peripheral.getNames()) do
        local ptype = peripheral.getType(name)
        if ptype == "playerDetector" then
            table.insert(player_detectors, {
                name = name,
                periph = peripheral.wrap(name),
            })
        end
    end
end
scan_player_detectors()
if #player_detectors > 0 then
    print("Player detectors: " .. #player_detectors)
end

-- Helper: get player detector peripheral by name
local function get_player_detector(periph_name)
    if not periph_name then return nil end
    for _, pd in ipairs(player_detectors) do
        if pd.name == periph_name then
            return pd.periph
        end
    end
    return nil
end

-- Check if any players are near this station
local players_nearby = false
local function check_players_nearby()
    local pd = get_player_detector(station_config.player_detector)
    if not pd then
        players_nearby = false
        return false
    end
    local ok, players = pcall(pd.getPlayersInRange, PLAYER_DETECT_RANGE)
    if ok and players and #players > 0 then
        players_nearby = true
        return true
    end
    players_nearby = false
    return false
end

-- ========================================
-- Interactive Setup
-- ========================================
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

    -- Hub mode
    print("")
    write("Is this the HUB station? [y/N]: ")
    local hub_ans = read()
    station_config.is_hub = (hub_ans == "y" or hub_ans == "Y")

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
            write("Is this a parking bay switch? [y/N]: ")
            local is_parking = read()
            local sw_entry = {
                peripheral_name = sw_periph,
                face = sw_face,
                description = sw_desc,
                state = false,
                routes = {},
                parking = (is_parking == "y" or is_parking == "Y"),
            }
            if sw_entry.parking then
                print("")
                print("-- Bay Detector Rail --")
                sw_entry.bay_detector_periph = pick_integrator("BAY DETECTOR RAIL (input)")
                sw_entry.bay_detector_face = pick_face("bay detector face", "top")
                print("")
                print("-- Bay Powered Rail --")
                sw_entry.bay_rail_periph = pick_integrator("BAY POWERED RAIL (output)")
                sw_entry.bay_rail_face = pick_face("bay powered rail face", "top")
                sw_entry.bay_has_train = false
            end
            table.insert(station_config.switches, sw_entry)
            local tag = sw_entry.parking and " (PARKING)" or ""
            print("Added: " .. sw_desc .. " [" .. sw_periph .. ":" .. sw_face .. "]" .. tag)
            write("Add another switch? [y/N]: ")
            local more = read()
            if more ~= "y" and more ~= "Y" then break end
        end
    end

    -- Player detector
    scan_player_detectors()
    if #player_detectors > 0 then
        print("")
        print("Found " .. #player_detectors .. " player detector(s):")
        for i, pd in ipairs(player_detectors) do
            print(string.format("  %d. %s", i, pd.name))
        end
        write("Use player detector? [Y/n]: ")
        local pd_ans = read()
        if pd_ans ~= "n" and pd_ans ~= "N" then
            if #player_detectors == 1 then
                station_config.player_detector = player_detectors[1].name
            else
                local idx = pick_number("Choice [1-" .. #player_detectors .. "]: ", #player_detectors)
                station_config.player_detector = player_detectors[idx].name
            end
            print("Player detector: " .. station_config.player_detector)
        else
            station_config.player_detector = nil
        end
    end

    save_config()

    print("")
    print("========================================")
    print("  Setup complete! Config saved.")
    print("========================================")
    print("  Mode:     " .. (station_config.is_hub and "HUB" or "REMOTE"))
    print("  Rail:     " .. station_config.rail_periph .. ":" .. station_config.rail_face)
    print("  Detector: " .. station_config.detector_periph .. ":" .. station_config.detector_face)
    print("  Switches: " .. #station_config.switches)
    print("  Players:  " .. (station_config.player_detector or "NONE"))
    print("========================================")
    sleep(1)
end

-- Run setup if unconfigured or requested via command line arg
local args = {...}
if args[1] == "setup" then
    run_setup()
elseif args[1] == "name" then
    local name = table.concat(args, " ", 2)
    if name ~= "" then
        station_config.label = name
        save_config()
        print("Station renamed to: " .. station_config.label)
    else
        print("Usage: station_client name <name>")
    end
    return
elseif args[1] == "hub" then
    station_config.is_hub = true
    save_config()
    print("Set as HUB station. Restart to apply.")
    return
elseif args[1] == "remote" then
    station_config.is_hub = false
    save_config()
    print("Set as REMOTE station. Restart to apply.")
    return
elseif not station_config.rail_periph or not station_config.detector_periph then
    if #redstone_integrators > 0 then
        run_setup()
    else
        print("WARNING: No integrators found and none configured!")
        print("Run 'station_client setup' after connecting peripherals.")
    end
end

-- Update HUB_ID after config load/setup
if station_config.is_hub then
    HUB_ID = os.getComputerID()
end

-- ========================================
-- Powered Rail Control (via integrator)
-- ========================================
-- Default: unpowered (brake). Power briefly to dispatch.

local has_train = false   -- detected by detector rail integrator

-- Departure / request state
local pending_destination = nil   -- {id, label} destination user selected
local pending_outbound = nil      -- hub only: {station_id, label} station requesting a train
local departure_countdown = nil   -- seconds remaining in countdown, nil if inactive

-- Per-bay state tracking for parking bays
-- Keyed by switch index: {last_signal, last_toggle_time, has_train}
local bay_states = {}

local function init_bay_states()
    for i, sw in ipairs(station_config.switches) do
        if sw.parking and sw.bay_detector_periph then
            if not bay_states[i] then
                bay_states[i] = {
                    last_signal = false,
                    last_toggle_time = 0,
                    has_train = sw.bay_has_train or false,
                }
            end
        end
    end
end
init_bay_states()

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

-- Bay powered rail control
local function bay_brake_on(sw_idx)
    local sw = station_config.switches[sw_idx]
    if not sw or not sw.bay_rail_periph then return end
    local ri = get_integrator(sw.bay_rail_periph)
    if ri then
        pcall(ri.setOutput, sw.bay_rail_face, false)
    end
end

local function dispatch_from_bay(sw_idx)
    local sw = station_config.switches[sw_idx]
    if not sw or not sw.bay_rail_periph then
        print("ERROR: Bay " .. sw_idx .. " has no rail config")
        return
    end
    local ri = get_integrator(sw.bay_rail_periph)
    if not ri then
        print("ERROR: Bay rail integrator not found: " .. tostring(sw.bay_rail_periph))
        return
    end
    print(string.format("Bay %d dispatch: %s:%s -> ON", sw_idx, sw.bay_rail_periph, sw.bay_rail_face))
    pcall(ri.setOutput, sw.bay_rail_face, true)
    os.sleep(DISPATCH_PULSE)
    pcall(ri.setOutput, sw.bay_rail_face, false)
    if bay_states[sw_idx] then
        bay_states[sw_idx].has_train = false
    end
    sw.bay_has_train = false
    save_config()
    print("Bay " .. sw_idx .. " dispatch complete, rail braked")
end

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

-- Auto-park: find empty bay, set switches, dispatch
local function auto_park()
    if not station_config.is_hub then return end
    if not has_train then return end

    local empty_bay = nil
    for i, sw in ipairs(station_config.switches) do
        if sw.parking then
            local bs = bay_states[i]
            if not bs or not bs.has_train then
                empty_bay = i
                break
            end
        end
    end
    if not empty_bay then
        print("[auto-park] No empty bays!")
        return
    end

    -- Set switches: target bay switch OFF (allow into bay from hub), others ON (block)
    for i, sw in ipairs(station_config.switches) do
        if sw.parking then
            set_switch(i, i ~= empty_bay)
        end
    end

    print("[auto-park] -> Bay " .. empty_bay .. " (" .. (station_config.switches[empty_bay].description or "?") .. ")")
    dispatch()
end

-- Start with brake on (station + all bays)
brake_on()
for i, sw in ipairs(station_config.switches) do
    if sw.parking and sw.bay_rail_periph then
        bay_brake_on(i)
    end
end

-- ========================================
-- Detector Rail Monitoring (via integrator)
-- ========================================
local last_signal = false    -- previous detector reading (for edge detection)
local last_toggle_time = 0   -- debounce: time of last toggle
local DEBOUNCE_TIME = 2      -- ignore rising edges for N seconds after a toggle

local function check_detector()
    if not station_config.detector_periph then return false end

    local ri = get_integrator(station_config.detector_periph)
    if not ri then return false end

    local ok, signal = pcall(ri.getInput, station_config.detector_face)
    if not ok then return false end

    -- Toggle on rising edge with debounce
    local now = os.clock()
    if signal and not last_signal then
        if (now - last_toggle_time) >= DEBOUNCE_TIME then
            has_train = not has_train
            last_toggle_time = now
            if has_train then
                print("[det] >>> TRAIN ARRIVED <<<")
                os.queueEvent("train_arrived")
            else
                print("[det] >>> TRAIN DEPARTED <<<")
                os.queueEvent("train_departed")
            end
            -- Notify hub immediately if remote station
            if HUB_ID and HUB_ID ~= os.getComputerID() then
                check_players_nearby()
                rednet.send(HUB_ID, {
                    id = os.getComputerID(),
                    label = station_config.label,
                    has_train = has_train,
                    players_nearby = players_nearby,
                }, PROTOCOLS.heartbeat)
            end
        end
    end
    last_signal = signal
    return true
end

-- Per-bay detector check (same toggle+debounce logic)
local function check_bay_detector(sw_idx)
    local sw = station_config.switches[sw_idx]
    if not sw or not sw.bay_detector_periph then return false end

    local state = bay_states[sw_idx]
    if not state then
        bay_states[sw_idx] = {last_signal = false, last_toggle_time = 0, has_train = false}
        state = bay_states[sw_idx]
    end

    local ri = get_integrator(sw.bay_detector_periph)
    if not ri then return false end

    local ok, signal = pcall(ri.getInput, sw.bay_detector_face)
    if not ok then return false end

    local now = os.clock()
    if signal and not state.last_signal then
        if (now - state.last_toggle_time) >= DEBOUNCE_TIME then
            state.has_train = not state.has_train
            state.last_toggle_time = now
            -- Persist bay state
            sw.bay_has_train = state.has_train
            save_config()
            if state.has_train then
                print("[bay " .. sw_idx .. "] >>> TRAIN PARKED <<<")
            else
                print("[bay " .. sw_idx .. "] >>> BAY EMPTY <<<")
            end
        end
    end
    state.last_signal = signal
    return true
end

-- Initial detector check
check_detector()

-- ========================================
-- Monitor Display
-- ========================================
local route_data = nil   -- station list from hub (for remote stations)

-- Monitor UI state
local monitor_mode = "main"  -- "main", "config", "pick_integrator", "pick_face", "switch_face", "switch_parking", "pick_player_det"
local config_purpose = nil   -- "rail", "detector", "switch", "bay_detector", "bay_rail"
local pending_switch = nil   -- temp switch being built
local monitor_buttons = {}   -- rebuilt each render

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
    local title = station_config.label
    if station_config.is_hub then title = "[HUB] " .. title end
    monitor.write(title:sub(1, mw - 8))
    local setup_lbl = "[SETUP]"
    monitor.setCursorPos(mw - #setup_lbl + 1, 1)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.white)
    monitor.write(setup_lbl)
    monitor.setBackgroundColor(colors.black)
    mon_btn(1, 1, "open_config", {x1 = mw - #setup_lbl + 1, x2 = mw})

    -- Status line
    monitor.setCursorPos(1, 2)
    if station_config.is_hub then
        local count = 0
        for _ in pairs(connected_stations) do count = count + 1 end
        monitor.setTextColor(colors.lime)
        monitor.write("HUB - " .. count .. " stations")
    elseif HUB_ID then
        monitor.setTextColor(colors.lime)
        monitor.write("Hub: #" .. HUB_ID)
    else
        monitor.setTextColor(colors.orange)
        monitor.write("Searching for hub...")
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

    -- Player presence indicator
    if station_config.player_detector then
        local player_x = mw - 10
        if player_x > 1 then
            monitor.setCursorPos(player_x, 3)
            if players_nearby then
                monitor.setTextColor(colors.yellow)
                monitor.write(" PLAYERS")
            else
                monitor.setTextColor(colors.gray)
                monitor.write("        ")
            end
        end
    end

    -- Separator
    monitor.setCursorPos(1, 4)
    monitor.setTextColor(colors.gray)
    monitor.write(string.rep("-", mw))

    local btn_y = 5

    if pending_destination or (station_config.is_hub and pending_outbound) then
        -- Departure / outbound state
        if station_config.is_hub and pending_outbound and not pending_destination then
            -- Hub sending train to a remote station
            monitor.setCursorPos(1, btn_y)
            monitor.setTextColor(colors.cyan)
            monitor.write("SENDING TRAIN TO:")
            btn_y = btn_y + 1
            monitor.setCursorPos(2, btn_y)
            monitor.setTextColor(colors.white)
            monitor.write(pending_outbound.label)
            btn_y = btn_y + 2
            monitor.setCursorPos(2, btn_y)
            monitor.setTextColor(colors.orange)
            monitor.write("Pulling from bay...")

        elseif pending_destination and departure_countdown then
            -- Countdown active
            monitor.setCursorPos(1, btn_y)
            monitor.setTextColor(colors.cyan)
            monitor.write("DEPARTING TO:")
            btn_y = btn_y + 1
            monitor.setCursorPos(2, btn_y)
            monitor.setTextColor(colors.white)
            monitor.write(pending_destination.label)
            btn_y = btn_y + 2

            -- Big countdown display
            local count_str = tostring(departure_countdown)
            local display = ">>> " .. count_str .. " <<<"
            monitor.setCursorPos(math.max(1, math.floor((mw - #display) / 2) + 1), btn_y)
            monitor.setTextColor(colors.yellow)
            monitor.write(display)
            btn_y = btn_y + 2

            -- Cancel button
            local cancel_text = " CANCEL "
            monitor.setCursorPos(math.max(1, math.floor((mw - #cancel_text) / 2) + 1), btn_y)
            monitor.setBackgroundColor(colors.red)
            monitor.setTextColor(colors.white)
            monitor.write(cancel_text)
            monitor.setBackgroundColor(colors.black)
            mon_btn(btn_y, btn_y, "cancel_departure", {})

        elseif pending_destination then
            -- Waiting for train
            monitor.setCursorPos(1, btn_y)
            monitor.setTextColor(colors.cyan)
            monitor.write("DESTINATION:")
            btn_y = btn_y + 1
            monitor.setCursorPos(2, btn_y)
            monitor.setTextColor(colors.white)
            monitor.write(pending_destination.label)
            btn_y = btn_y + 2
            monitor.setCursorPos(2, btn_y)
            monitor.setTextColor(colors.orange)
            monitor.write("Requesting train...")
            btn_y = btn_y + 2

            -- Cancel button
            local cancel_text = " CANCEL "
            monitor.setCursorPos(math.max(1, math.floor((mw - #cancel_text) / 2) + 1), btn_y)
            monitor.setBackgroundColor(colors.red)
            monitor.setTextColor(colors.white)
            monitor.write(cancel_text)
            monitor.setBackgroundColor(colors.black)
            mon_btn(btn_y, btn_y, "cancel_departure", {})
        end

    elseif station_config.is_hub then
        -- Hub overview: parking bays + connected stations + destinations
        monitor.setCursorPos(1, btn_y)
        monitor.setTextColor(colors.cyan)
        monitor.write("PARKING BAYS:")
        btn_y = btn_y + 1

        local has_bays = false
        for si, sw in ipairs(station_config.switches) do
            if sw.parking then
                has_bays = true
                if btn_y > mh - 4 then break end
                local bs = bay_states[si]
                local parked = bs and bs.has_train
                monitor.setCursorPos(2, btn_y)
                if parked then
                    monitor.setTextColor(colors.lime)
                    monitor.write(string.format("%s [PARKED]", sw.description or "Bay " .. si))
                else
                    monitor.setTextColor(colors.gray)
                    monitor.write(string.format("%s [EMPTY]", sw.description or "Bay " .. si))
                end
                btn_y = btn_y + 1
            end
        end
        if not has_bays then
            monitor.setCursorPos(2, btn_y)
            monitor.setTextColor(colors.gray)
            monitor.write("No parking bays configured")
            btn_y = btn_y + 1
        end

        -- Separator
        btn_y = btn_y + 1
        if btn_y <= mh then
            monitor.setCursorPos(1, btn_y)
            monitor.setTextColor(colors.gray)
            monitor.write(string.rep("-", mw))
            btn_y = btn_y + 1
        end

        -- Connected stations
        if btn_y <= mh then
            monitor.setCursorPos(1, btn_y)
            monitor.setTextColor(colors.cyan)
            monitor.write("STATIONS:")
            btn_y = btn_y + 1
        end

        local sorted = {}
        for id, st in pairs(connected_stations) do
            table.insert(sorted, st)
        end
        table.sort(sorted, function(a, b) return (a.label or "") < (b.label or "") end)

        for _, st in ipairs(sorted) do
            if btn_y > mh then break end
            monitor.setCursorPos(2, btn_y)
            local online = st.online and (os.clock() - st.last_seen) < 15
            monitor.setTextColor(online and colors.white or colors.red)
            local tags = ""
            if st.has_train then tags = tags .. " [TRAIN]" end
            if st.players_nearby then tags = tags .. " [P]" end
            monitor.write((st.label or "#" .. st.id):sub(1, mw - 16) .. tags)
            btn_y = btn_y + 1
        end

        if #sorted == 0 and btn_y <= mh then
            monitor.setCursorPos(2, btn_y)
            monitor.setTextColor(colors.gray)
            monitor.write("No stations connected")
            btn_y = btn_y + 1
        end

        -- Separator before destinations
        if next(connected_stations) and btn_y + 2 <= mh then
            btn_y = btn_y + 1
            monitor.setCursorPos(1, btn_y)
            monitor.setTextColor(colors.gray)
            monitor.write(string.rep("-", mw))
            btn_y = btn_y + 1

            -- Hub destination buttons
            monitor.setCursorPos(1, btn_y)
            monitor.setTextColor(colors.cyan)
            monitor.write("GO TO:")
            btn_y = btn_y + 1

            for id, st in pairs(connected_stations) do
                if btn_y + 1 > mh then break end
                local lbl = st.label or ("Station #" .. id)
                monitor.setCursorPos(2, btn_y)
                monitor.setBackgroundColor(colors.blue)
                monitor.setTextColor(colors.white)
                local btn_text = " " .. lbl:sub(1, mw - 4) .. string.rep(" ", math.max(0, mw - 4 - #lbl))
                monitor.write(btn_text)
                monitor.setBackgroundColor(colors.black)
                mon_btn(btn_y, btn_y, "dispatch_to", {id = id, label = lbl})
                btn_y = btn_y + 2
            end
        end

    else
        -- Remote station: destination buttons (always clickable)
        monitor.setCursorPos(1, btn_y)
        monitor.setTextColor(colors.cyan)
        monitor.write("DESTINATIONS:")
        btn_y = btn_y + 1

        if route_data then
            for id, st in pairs(route_data) do
                if tostring(id) ~= tostring(os.getComputerID()) and btn_y <= mh then
                    local lbl = st.label or ("Station #" .. id)
                    if st.is_hub then lbl = "\4 " .. lbl end

                    monitor.setCursorPos(2, btn_y)
                    monitor.setBackgroundColor(colors.blue)
                    monitor.setTextColor(colors.white)
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
    end

    -- Switch status at bottom (non-parking switches only)
    local non_parking = {}
    for si, sw in ipairs(station_config.switches) do
        if not sw.parking then table.insert(non_parking, {idx = si, sw = sw}) end
    end
    if #non_parking > 0 then
        local sy = mh - #non_parking
        if sy < btn_y + 1 then sy = btn_y + 1 end
        if sy <= mh then
            monitor.setCursorPos(1, sy)
            monitor.setTextColor(colors.cyan)
            monitor.write("SWITCHES:")
            for i, entry in ipairs(non_parking) do
                if sy + i <= mh then
                    monitor.setCursorPos(2, sy + i)
                    monitor.setTextColor(entry.sw.state and colors.lime or colors.gray)
                    monitor.write(string.format("%d. %s [%s]",
                        entry.idx, entry.sw.description or entry.sw.face, entry.sw.state and "ON" or "OFF"))
                end
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

    -- Hub toggle
    monitor.setCursorPos(1, cy)
    monitor.setTextColor(colors.white)
    monitor.write("Mode: ")
    if station_config.is_hub then
        monitor.setBackgroundColor(colors.green)
        monitor.setTextColor(colors.white)
        monitor.write(" HUB ")
    else
        monitor.setBackgroundColor(colors.blue)
        monitor.setTextColor(colors.white)
        monitor.write(" REMOTE ")
    end
    monitor.setBackgroundColor(colors.black)
    mon_btn(cy, cy, "toggle_hub", {})
    cy = cy + 1

    -- Rail integrator
    cy = cy + 1
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

    -- Switches section
    cy = cy + 1
    monitor.setCursorPos(1, cy)
    monitor.setTextColor(colors.white)
    monitor.write("Switches:")
    local add_lbl = "[+ADD]"
    monitor.setCursorPos(mw - #add_lbl + 1, cy)
    monitor.setBackgroundColor(colors.green)
    monitor.setTextColor(colors.white)
    monitor.write(add_lbl)
    monitor.setBackgroundColor(colors.black)
    mon_btn(cy, cy, "add_switch_start", {x1 = mw - #add_lbl + 1, x2 = mw})
    cy = cy + 1

    for si, sw in ipairs(station_config.switches) do
        if cy > mh - 3 then break end
        monitor.setCursorPos(2, cy)
        local tag = ""
        if sw.parking then
            local bs = bay_states[si]
            local parked = bs and bs.has_train
            tag = parked and " P*" or " P"
        end
        monitor.setTextColor(sw.state and colors.lime or colors.yellow)
        monitor.write(string.format("%d.%s %s [%s]",
            si, tag, (sw.description or "Switch"):sub(1, mw - 14),
            sw.state and "ON" or "OFF"))
        -- Delete button
        monitor.setCursorPos(mw - 2, cy)
        monitor.setBackgroundColor(colors.red)
        monitor.setTextColor(colors.white)
        monitor.write("[X]")
        monitor.setBackgroundColor(colors.black)
        mon_btn(cy, cy, "remove_switch", {idx = si, x1 = mw - 2, x2 = mw})
        cy = cy + 1
    end
    if #station_config.switches == 0 then
        monitor.setCursorPos(2, cy)
        monitor.setTextColor(colors.gray)
        monitor.write("None")
        cy = cy + 1
    end

    -- Player Detector
    cy = cy + 1
    if cy <= mh then
        monitor.setCursorPos(1, cy)
        monitor.setTextColor(colors.white)
        monitor.write("Player Detect:")
        cy = cy + 1
        if cy <= mh then
            monitor.setCursorPos(2, cy)
            if station_config.player_detector then
                monitor.setTextColor(colors.yellow)
                monitor.write(station_config.player_detector:sub(1, mw - 12))
                -- Remove button
                monitor.setCursorPos(mw - 2, cy)
                monitor.setBackgroundColor(colors.red)
                monitor.setTextColor(colors.white)
                monitor.write("[X]")
                monitor.setBackgroundColor(colors.black)
                mon_btn(cy, cy, "remove_player_det", {x1 = mw - 2, x2 = mw})
            else
                monitor.setTextColor(colors.gray)
                monitor.write("NONE")
                local set_lbl = "[SET]"
                monitor.setCursorPos(mw - #set_lbl + 1, cy)
                monitor.setBackgroundColor(colors.blue)
                monitor.setTextColor(colors.white)
                monitor.write(set_lbl)
                monitor.setBackgroundColor(colors.black)
                mon_btn(cy, cy, "pick_player_det", {x1 = mw - #set_lbl + 1, x2 = mw})
            end
        end
        cy = cy + 1
    end

    -- Rescan
    cy = cy + 1
    if cy <= mh then
        monitor.setCursorPos(1, cy)
        monitor.setTextColor(colors.gray)
        monitor.write("Integrators: " .. #redstone_integrators)
        local rescan_lbl = "[RESCAN]"
        monitor.setCursorPos(mw - #rescan_lbl + 1, cy)
        monitor.setBackgroundColor(colors.gray)
        monitor.setTextColor(colors.white)
        monitor.write(rescan_lbl)
        monitor.setBackgroundColor(colors.black)
        mon_btn(cy, cy, "rescan", {x1 = mw - #rescan_lbl + 1, x2 = mw})
    end
end

local function render_integrator_picker()
    if not monitor then return end

    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    monitor.setCursorPos(1, 1)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.white)
    monitor.write("< BACK")
    monitor.setBackgroundColor(colors.black)
    mon_btn(1, 1, "back_to_config", {x1 = 1, x2 = 6})

    local purpose_labels = {
        rail = "POWERED RAIL",
        detector = "DETECTOR RAIL",
        switch = "SWITCH",
        bay_detector = "BAY DETECTOR",
        bay_rail = "BAY POWERED RAIL",
    }
    local purpose_lbl = purpose_labels[config_purpose] or config_purpose
    monitor.setCursorPos(1, 2)
    monitor.setTextColor(colors.cyan)
    monitor.write("SELECT FOR: " .. purpose_lbl)

    monitor.setCursorPos(1, 3)
    monitor.setTextColor(colors.gray)
    monitor.write(string.rep("-", mw))

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
            else
                monitor.setBackgroundColor(colors.gray)
            end
            monitor.setTextColor(colors.white)
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

    monitor.setCursorPos(1, 1)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.white)
    monitor.write("< BACK")
    monitor.setBackgroundColor(colors.black)
    mon_btn(1, 1, "back_to_config", {x1 = 1, x2 = 6})

    local purpose_labels = {
        rail = "POWERED RAIL",
        detector = "DETECTOR RAIL",
        switch = "SWITCH",
        bay_detector = "BAY DETECTOR",
        bay_rail = "BAY POWERED RAIL",
    }
    local purpose_lbl = purpose_labels[config_purpose] or config_purpose
    monitor.setCursorPos(1, 2)
    monitor.setTextColor(colors.cyan)
    monitor.write("SELECT FACE: " .. purpose_lbl)

    monitor.setCursorPos(1, 3)
    monitor.setTextColor(colors.gray)
    monitor.write(string.rep("-", mw))

    local current_face
    if config_purpose == "rail" then
        current_face = station_config.rail_face
    elseif config_purpose == "detector" then
        current_face = station_config.detector_face
    else
        current_face = nil
    end

    local cy = 4
    for i, face in ipairs(FACES) do
        if cy > mh then break end
        local is_current = (face == current_face)

        monitor.setCursorPos(1, cy)
        if is_current then
            monitor.setBackgroundColor(colors.blue)
        else
            monitor.setBackgroundColor(colors.gray)
        end
        monitor.setTextColor(colors.white)
        local entry = string.format(" %d. %s %s", i, face, is_current and "*" or " ")
        monitor.write(entry .. string.rep(" ", math.max(0, mw - #entry)))
        monitor.setBackgroundColor(colors.black)
        mon_btn(cy, cy, "select_face", {face = face})
        cy = cy + 1
    end
end

local function render_switch_parking()
    if not monitor then return end

    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    monitor.setCursorPos(1, 1)
    monitor.setTextColor(colors.cyan)
    monitor.write("NEW SWITCH")

    monitor.setCursorPos(1, 3)
    monitor.setTextColor(colors.white)
    monitor.write("Integrator: " .. (pending_switch and pending_switch.peripheral_name or "?"))
    monitor.setCursorPos(1, 4)
    monitor.write("Face: " .. (pending_switch and pending_switch.face or "?"))

    monitor.setCursorPos(1, 6)
    monitor.setTextColor(colors.cyan)
    monitor.write("Is this a parking switch?")

    monitor.setCursorPos(2, 8)
    monitor.setBackgroundColor(colors.green)
    monitor.setTextColor(colors.white)
    monitor.write(" YES - PARKING ")
    monitor.setBackgroundColor(colors.black)
    mon_btn(8, 8, "finish_switch", {parking = true})

    monitor.setCursorPos(2, 10)
    monitor.setBackgroundColor(colors.blue)
    monitor.setTextColor(colors.white)
    monitor.write(" NO - REGULAR  ")
    monitor.setBackgroundColor(colors.black)
    mon_btn(10, 10, "finish_switch", {parking = false})
end

local function render_player_detector_picker()
    if not monitor then return end

    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    monitor.setCursorPos(1, 1)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.white)
    monitor.write("< BACK")
    monitor.setBackgroundColor(colors.black)
    mon_btn(1, 1, "back_to_config", {x1 = 1, x2 = 6})

    monitor.setCursorPos(1, 2)
    monitor.setTextColor(colors.cyan)
    monitor.write("SELECT PLAYER DETECTOR")

    monitor.setCursorPos(1, 3)
    monitor.setTextColor(colors.gray)
    monitor.write(string.rep("-", mw))

    scan_player_detectors()

    local cy = 4
    if #player_detectors == 0 then
        monitor.setCursorPos(2, cy)
        monitor.setTextColor(colors.red)
        monitor.write("No player detectors found!")
    else
        for i, pd in ipairs(player_detectors) do
            if cy > mh then break end
            local is_current = (pd.name == station_config.player_detector)
            monitor.setCursorPos(1, cy)
            if is_current then
                monitor.setBackgroundColor(colors.blue)
            else
                monitor.setBackgroundColor(colors.gray)
            end
            monitor.setTextColor(colors.white)
            local entry = string.format(" %d. %s %s", i, pd.name:sub(1, mw - 8), is_current and "*" or " ")
            monitor.write(entry .. string.rep(" ", math.max(0, mw - #entry)))
            monitor.setBackgroundColor(colors.black)
            mon_btn(cy, cy, "select_player_det", {name = pd.name})
            cy = cy + 1
        end
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
    elseif monitor_mode == "switch_parking" then
        render_switch_parking()
    elseif monitor_mode == "pick_player_det" then
        render_player_detector_picker()
    else
        render_main_monitor()
    end
end

-- ========================================
-- Update Check (GitHub HTTP)
-- ========================================
local function check_for_updates()
    if not http then return false end
    print("[update] Checking github...")
    local ok, resp, err = pcall(http.get, UPDATE_URL)
    if not ok or not resp then return false end
    local code = resp.getResponseCode()
    local content = resp.readAll()
    resp.close()
    if code ~= 200 or not content or #content < 100 then return false end
    local sum = 0
    for i = 1, #content do
        sum = (sum * 31 + string.byte(content, i)) % 2147483647
    end
    local remote_ver = tostring(sum)
    if remote_ver == VERSION then return false end
    print("[update] New version! Updating...")
    local path = shell.getRunningProgram()
    local f = fs.open(path, "w")
    if not f then return false end
    f.write(content)
    f.close()
    print("[update] Rebooting...")
    sleep(0.5)
    os.reboot()
end

check_for_updates()

-- ========================================
-- Hub Discovery (remote stations only)
-- ========================================
local function discover_hub()
    if station_config.is_hub then return true end
    rednet.broadcast({
        type = "station_ping",
        label = station_config.label,
        id = os.getComputerID(),
    }, PROTOCOLS.ping)

    local sender, msg = rednet.receive(PROTOCOLS.status, DISCOVERY_TIMEOUT)
    if sender and type(msg) == "table" and msg.status == "hub_ack" then
        HUB_ID = sender
        if msg.stations then
            route_data = msg.stations
        end
        print("Found hub at #" .. sender)
        return true
    end
    return false
end

local function register_with_hub()
    if not HUB_ID or station_config.is_hub then return false end
    rednet.send(HUB_ID, {
        label = station_config.label,
        id = os.getComputerID(),
        x = my_x, y = my_y, z = my_z,
        has_train = has_train,
        switches = station_config.switches,
    }, PROTOCOLS.register)
    local sender, msg = rednet.receive(PROTOCOLS.status, 3)
    if sender == HUB_ID and type(msg) == "table" and msg.status == "registered" then
        print("Registered with hub #" .. HUB_ID)
        return true
    end
    return false
end

-- ========================================
-- Hub Station List Helper
-- ========================================
local function get_station_list()
    -- Build station list for broadcasting to remote stations
    local stations = {}
    -- Include self (hub)
    stations[os.getComputerID()] = {
        label = station_config.label,
        is_hub = true,
        x = my_x, y = my_y, z = my_z,
        has_train = has_train,
    }
    -- Include connected stations
    for id, st in pairs(connected_stations) do
        stations[id] = {
            label = st.label,
            is_hub = false,
            x = st.x or 0, y = st.y or 0, z = st.z or 0,
            has_train = st.has_train,
        }
    end
    return stations
end

-- ========================================
-- Initial Connection
-- ========================================
if not station_config.is_hub then
    print("Searching for hub...")
    discover_hub()
    if HUB_ID then
        register_with_hub()
    else
        print("Hub not found - running standalone")
    end
end

-- ========================================
-- Display status
-- ========================================
term.clear()
term.setCursorPos(1, 1)
print("=== Station Client v" .. VERSION .. " ===")
print("Computer #" .. os.getComputerID())
print("Station:    " .. station_config.label)
print("Mode:       " .. (station_config.is_hub and "HUB" or "REMOTE"))
if not station_config.is_hub then
    print("Hub:        " .. (HUB_ID and ("#" .. HUB_ID) or "NOT FOUND"))
end
print("Position:   " .. my_x .. ", " .. my_y .. ", " .. my_z)
print("Rail:       " .. tostring(station_config.rail_periph) .. ":" .. station_config.rail_face)
print("Detector:   " .. tostring(station_config.detector_periph) .. ":" .. station_config.detector_face)
print("Integrators:" .. #redstone_integrators)
print("Switches:   " .. #station_config.switches)
print("Monitor:    " .. (monitor and "YES" or "NO"))
print("")
print("Listening...")
print("")

-- ========================================
-- Main Loops
-- ========================================

local function command_listener()
    while true do
        local sender, msg, proto = rednet.receive(nil, 1)

        if sender and type(msg) == "table" then

            -- Hub: handle pings from remote stations
            if station_config.is_hub and proto == PROTOCOLS.ping then
                rednet.send(sender, {
                    status = "hub_ack",
                    stations = get_station_list(),
                }, PROTOCOLS.status)

            -- Hub: handle registration from remote stations
            elseif station_config.is_hub and proto == PROTOCOLS.register then
                connected_stations[sender] = {
                    id = sender,
                    label = msg.label or ("Station #" .. sender),
                    x = msg.x or 0,
                    y = msg.y or 0,
                    z = msg.z or 0,
                    has_train = msg.has_train or false,
                    switches = msg.switches or {},
                    online = true,
                    last_seen = os.clock(),
                }
                print("Station registered: " .. (msg.label or "#" .. sender))
                rednet.send(sender, {
                    status = "registered",
                }, PROTOCOLS.status)
                -- Broadcast updated station list to all connected stations
                local stations = get_station_list()
                for id, _ in pairs(connected_stations) do
                    rednet.send(id, {stations = stations}, PROTOCOLS.status)
                end

            -- Hub: handle heartbeats from remote stations
            elseif station_config.is_hub and proto == PROTOCOLS.heartbeat and type(msg) == "table" then
                local st = connected_stations[sender]
                if st then
                    st.last_seen = os.clock()
                    st.online = true
                    if msg.has_train ~= nil then st.has_train = msg.has_train end
                    if msg.players_nearby ~= nil then st.players_nearby = msg.players_nearby end
                    if msg.label then st.label = msg.label end
                end

            -- Remote: handle status updates from hub
            elseif not station_config.is_hub and proto == PROTOCOLS.status then
                if type(msg) == "table" and msg.stations then
                    route_data = msg.stations
                end

            -- Both: handle direct commands
            elseif proto == PROTOCOLS.command then
                if msg.action == "dispatch" then
                    print(string.format("DISPATCH to %s", msg.destination_label or "?"))
                    dispatch()

                elseif msg.action == "set_switch" then
                    if msg.switch_idx then
                        set_switch(msg.switch_idx, msg.state)
                    end

                elseif msg.action == "brake" then
                    brake_on()

                elseif msg.action == "dispatch_from_bay" then
                    if msg.switch_idx and station_config.switches[msg.switch_idx] then
                        print(string.format("BAY %d DISPATCH", msg.switch_idx))
                        dispatch_from_bay(msg.switch_idx)
                    end

                elseif msg.action == "set_rail" then
                    station_config.rail_periph = msg.peripheral_name or station_config.rail_periph
                    station_config.rail_face = msg.face or station_config.rail_face
                    save_config()
                    brake_on()

                elseif msg.action == "set_detector" then
                    station_config.detector_periph = msg.peripheral_name or station_config.detector_periph
                    station_config.detector_face = msg.face or station_config.detector_face
                    save_config()

                elseif msg.action == "set_label" then
                    station_config.label = msg.label
                    save_config()

                elseif msg.action == "add_switch" then
                    table.insert(station_config.switches, {
                        peripheral_name = msg.peripheral_name,
                        face = msg.face or "top",
                        description = msg.description or "Switch",
                        state = false,
                        routes = msg.routes or {},
                        parking = msg.parking or false,
                    })
                    save_config()

                elseif msg.action == "remove_switch" then
                    if station_config.switches[msg.idx] then
                        table.remove(station_config.switches, msg.idx)
                        save_config()
                    end

                elseif msg.action == "request_train" then
                    -- Remote station requesting a train (hub only)
                    if station_config.is_hub and not pending_outbound and not pending_destination then
                        pending_outbound = {
                            station_id = msg.station_id or sender,
                            label = msg.label or ("Station #" .. sender),
                        }
                        local bay_idx = nil
                        for i, sw in ipairs(station_config.switches) do
                            if sw.parking then
                                local bs = bay_states[i]
                                if bs and bs.has_train then
                                    bay_idx = i
                                    break
                                end
                            end
                        end
                        if bay_idx then
                            for i, sw in ipairs(station_config.switches) do
                                if sw.parking then
                                    set_switch(i, i ~= bay_idx)
                                end
                            end
                            print("[hub] Pulling from bay " .. bay_idx .. " for " .. pending_outbound.label)
                            dispatch_from_bay(bay_idx)
                        else
                            print("[hub] No trains available for " .. pending_outbound.label)
                            rednet.send(sender, {action = "train_unavailable"}, PROTOCOLS.command)
                            pending_outbound = nil
                        end
                    elseif station_config.is_hub then
                        -- Hub busy, reject
                        rednet.send(sender, {action = "train_unavailable"}, PROTOCOLS.command)
                    end

                elseif msg.action == "train_unavailable" then
                    -- Hub told us no trains available
                    if pending_destination and not has_train then
                        print("No trains available at hub")
                        pending_destination = nil
                        departure_countdown = nil
                    end
                end
            end
        end
    end
end

local function discovery_loop()
    if station_config.is_hub then
        -- Hub: check for offline stations
        while true do
            sleep(10)
            local now = os.clock()
            for id, st in pairs(connected_stations) do
                if st.online and st.last_seen > 0 and (now - st.last_seen) > 30 then
                    st.online = false
                    print("Station offline: " .. (st.label or "#" .. id))
                end
            end
        end
    else
        -- Remote: discover and maintain connection to hub
        local missed_pings = 0
        while true do
            if not HUB_ID then
                if discover_hub() then
                    register_with_hub()
                end
                sleep(DISCOVERY_INTERVAL)
            else
                sleep(60)
                check_players_nearby()
                rednet.send(HUB_ID, {
                    id = os.getComputerID(),
                    label = station_config.label,
                    has_train = has_train,
                    players_nearby = players_nearby,
                }, PROTOCOLS.heartbeat)
                local _, resp = rednet.receive(PROTOCOLS.status, 5)
                if not resp then
                    missed_pings = missed_pings + 1
                    if missed_pings >= 3 then
                        print("Lost hub connection. Retrying...")
                        HUB_ID = nil
                        missed_pings = 0
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
end

-- Handles all train arrivals: outbound dispatch, departure countdown, or auto-park
local function train_arrival_handler()
    while true do
        os.pullEvent("train_arrived")

        if station_config.is_hub and pending_outbound then
            -- Hub: train pulled from bay, dispatch to requesting station
            local target = pending_outbound
            pending_outbound = nil
            print("[hub] Train ready, sending to " .. (target.label or "#" .. target.station_id))
            os.sleep(1)
            if has_train then
                dispatch()
            end

        elseif pending_destination then
            -- Train arrived with a pending destination - trigger countdown
            os.queueEvent("departure_start")

        elseif station_config.is_hub then
            -- Hub: no pending jobs, auto-park after delay
            print("[auto-park] Train arrived, waiting " .. AUTO_PARK_DELAY .. "s...")
            local park_timer = os.startTimer(AUTO_PARK_DELAY)
            local should_park = true
            while true do
                local e, p1 = os.pullEvent()
                if e == "timer" and p1 == park_timer then
                    break
                elseif e == "train_departed" then
                    print("[auto-park] Cancelled (train dispatched)")
                    should_park = false
                    break
                elseif e == "destination_selected" then
                    print("[auto-park] Cancelled (destination selected)")
                    should_park = false
                    break
                end
            end
            if should_park and has_train then
                check_players_nearby()
                if players_nearby then
                    print("[auto-park] Players nearby, waiting for them to leave...")
                    while true do
                        if not has_train then
                            print("[auto-park] Cancelled (train departed while waiting)")
                            should_park = false
                            break
                        end
                        check_players_nearby()
                        if not players_nearby then
                            print("[auto-park] Players left, parking now...")
                            break
                        end
                        os.sleep(PLAYER_CHECK_INTERVAL)
                    end
                end
                if should_park and has_train then
                    print("[auto-park] Parking train...")
                    auto_park()
                end
            end
        end
    end
end

-- Handles the 30-second departure countdown
local function departure_handler()
    while true do
        os.pullEvent("departure_start")

        if not pending_destination then goto skip end

        departure_countdown = 30
        print("[depart] Countdown 30s -> " .. pending_destination.label)

        local tick = os.startTimer(1)
        local cancelled = false

        while departure_countdown > 0 do
            local e, p1 = os.pullEvent()
            if e == "timer" and p1 == tick then
                departure_countdown = departure_countdown - 1
                if departure_countdown > 0 then
                    tick = os.startTimer(1)
                end
            elseif e == "train_departed" then
                print("[depart] Cancelled (train left)")
                cancelled = true
                break
            elseif e == "cancel_departure" then
                print("[depart] Cancelled by user")
                cancelled = true
                break
            end
        end

        if not cancelled and has_train and pending_destination then
            local dest = pending_destination
            -- Notify hub about outbound dispatch
            if HUB_ID and HUB_ID ~= os.getComputerID() then
                rednet.send(HUB_ID, {
                    action = "request_dispatch",
                    from = os.getComputerID(),
                    to = dest.id,
                }, PROTOCOLS.command)
            end
            print("[depart] Dispatching to " .. dest.label)
            dispatch()
        end

        pending_destination = nil
        departure_countdown = nil

        ::skip::
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

        for _, btn in ipairs(monitor_buttons) do
            if ty >= btn.y1 and ty <= btn.y2 then
                if btn.data and btn.data.x1 then
                    if tx < btn.data.x1 or tx > btn.data.x2 then
                        goto continue
                    end
                end

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

                elseif btn.action == "toggle_hub" then
                    station_config.is_hub = not station_config.is_hub
                    if station_config.is_hub then
                        HUB_ID = os.getComputerID()
                    else
                        HUB_ID = nil
                    end
                    save_config()
                    render_monitor()

                elseif btn.action == "dispatch_to" then
                    if not pending_destination then
                        pending_destination = {id = btn.data.id, label = btn.data.label}
                        if has_train then
                            -- Train already here, start countdown
                            os.queueEvent("departure_start")
                            os.queueEvent("destination_selected")
                            print("Departing to " .. btn.data.label .. " in 30s")
                        else
                            -- No train, request one
                            if station_config.is_hub then
                                -- Hub: pull from parking bay
                                local bay_idx = nil
                                for i, sw in ipairs(station_config.switches) do
                                    if sw.parking then
                                        local bs = bay_states[i]
                                        if bs and bs.has_train then
                                            bay_idx = i
                                            break
                                        end
                                    end
                                end
                                if bay_idx then
                                    for i, sw in ipairs(station_config.switches) do
                                        if sw.parking then
                                            set_switch(i, i ~= bay_idx)
                                        end
                                    end
                                    print("[hub] Pulling from bay " .. bay_idx .. " for departure")
                                    dispatch_from_bay(bay_idx)
                                else
                                    print("No parked trains available!")
                                    pending_destination = nil
                                end
                            elseif HUB_ID then
                                -- Remote: ask hub for a train
                                rednet.send(HUB_ID, {
                                    action = "request_train",
                                    station_id = os.getComputerID(),
                                    label = station_config.label,
                                }, PROTOCOLS.command)
                                print("Requesting train from hub for " .. btn.data.label)
                            else
                                print("No hub connected!")
                                pending_destination = nil
                            end
                        end
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
                        save_config()
                        monitor_mode = "pick_face"
                    elseif config_purpose == "detector" then
                        station_config.detector_periph = btn.data.name
                        save_config()
                        monitor_mode = "pick_face"
                    elseif config_purpose == "switch" then
                        pending_switch = {peripheral_name = btn.data.name}
                        monitor_mode = "pick_face"
                    elseif config_purpose == "bay_detector" then
                        pending_switch.bay_detector_periph = btn.data.name
                        monitor_mode = "pick_face"
                    elseif config_purpose == "bay_rail" then
                        pending_switch.bay_rail_periph = btn.data.name
                        monitor_mode = "pick_face"
                    end
                    render_monitor()

                elseif btn.action == "select_face" then
                    if config_purpose == "rail" then
                        station_config.rail_face = btn.data.face
                        brake_on()
                        save_config()
                        monitor_mode = "config"
                        config_purpose = nil
                    elseif config_purpose == "detector" then
                        station_config.detector_face = btn.data.face
                        save_config()
                        monitor_mode = "config"
                        config_purpose = nil
                    elseif config_purpose == "switch" then
                        pending_switch.face = btn.data.face
                        monitor_mode = "switch_parking"
                    elseif config_purpose == "bay_detector" then
                        pending_switch.bay_detector_face = btn.data.face
                        config_purpose = "bay_rail"
                        monitor_mode = "pick_integrator"
                    elseif config_purpose == "bay_rail" then
                        pending_switch.bay_rail_face = btn.data.face
                        local sw_idx = #station_config.switches + 1
                        table.insert(station_config.switches, {
                            peripheral_name = pending_switch.peripheral_name,
                            face = pending_switch.face,
                            description = "Bay " .. sw_idx,
                            state = false,
                            routes = {},
                            parking = true,
                            bay_detector_periph = pending_switch.bay_detector_periph,
                            bay_detector_face = pending_switch.bay_detector_face,
                            bay_rail_periph = pending_switch.bay_rail_periph,
                            bay_rail_face = pending_switch.bay_rail_face,
                            bay_has_train = false,
                        })
                        save_config()
                        bay_states[sw_idx] = {
                            last_signal = false,
                            last_toggle_time = 0,
                            has_train = false,
                        }
                        bay_brake_on(sw_idx)
                        print("Parking bay added: " .. pending_switch.peripheral_name .. ":" .. pending_switch.face)
                        pending_switch = nil
                        config_purpose = nil
                        monitor_mode = "config"
                    end
                    render_monitor()

                elseif btn.action == "add_switch_start" then
                    config_purpose = "switch"
                    pending_switch = nil
                    monitor_mode = "pick_integrator"
                    render_monitor()

                elseif btn.action == "finish_switch" then
                    if pending_switch then
                        pending_switch.parking = btn.data.parking
                        if btn.data.parking then
                            config_purpose = "bay_detector"
                            monitor_mode = "pick_integrator"
                            render_monitor()
                        else
                            table.insert(station_config.switches, {
                                peripheral_name = pending_switch.peripheral_name,
                                face = pending_switch.face,
                                description = "Switch " .. (#station_config.switches + 1),
                                state = false,
                                routes = {},
                                parking = false,
                            })
                            save_config()
                            pending_switch = nil
                            config_purpose = nil
                            monitor_mode = "config"
                            render_monitor()
                        end
                    end

                elseif btn.action == "remove_switch" then
                    local idx = btn.data.idx
                    if station_config.switches[idx] then
                        print("Switch removed: " .. (station_config.switches[idx].description or "#" .. idx))
                        table.remove(station_config.switches, idx)
                        save_config()
                    end
                    render_monitor()

                elseif btn.action == "cancel_departure" then
                    print("Departure cancelled")
                    pending_destination = nil
                    departure_countdown = nil
                    os.queueEvent("cancel_departure")
                    render_monitor()

                elseif btn.action == "pick_player_det" then
                    monitor_mode = "pick_player_det"
                    render_monitor()

                elseif btn.action == "select_player_det" then
                    station_config.player_detector = btn.data.name
                    save_config()
                    print("Player detector set: " .. btn.data.name)
                    monitor_mode = "config"
                    render_monitor()

                elseif btn.action == "remove_player_det" then
                    station_config.player_detector = nil
                    players_nearby = false
                    save_config()
                    print("Player detector removed")
                    render_monitor()

                elseif btn.action == "rescan" then
                    scan_integrators()
                    scan_player_detectors()
                    print("Rescanned: " .. #redstone_integrators .. " integrators, " .. #player_detectors .. " player detectors")
                    render_monitor()
                end

                break
                ::continue::
            end
        end
    end
end

local function check_all_bay_detectors()
    for i, sw in ipairs(station_config.switches) do
        if sw.parking and sw.bay_detector_periph then
            check_bay_detector(i)
        end
    end
end

local function detector_loop()
    print("[detector] Loop started (poll=" .. DETECTOR_FALLBACK_POLL .. "s)")
    local bay_count = 0
    for i, sw in ipairs(station_config.switches) do
        if sw.parking and sw.bay_detector_periph then bay_count = bay_count + 1 end
    end
    print("[detector] Parking bays with detectors: " .. bay_count)
    local poll_timer = os.startTimer(DETECTOR_FALLBACK_POLL)

    while true do
        local event, p1, p2 = os.pullEvent()

        if event == "redstoneIntegrator" then
            if p2 == station_config.detector_periph then
                check_detector()
            end
            for i, sw in ipairs(station_config.switches) do
                if sw.parking and p2 == sw.bay_detector_periph then
                    check_bay_detector(i)
                end
            end
        elseif event == "redstone" then
            check_detector()
            check_all_bay_detectors()
        elseif event == "timer" and p1 == poll_timer then
            check_detector()
            check_all_bay_detectors()
            poll_timer = os.startTimer(DETECTOR_FALLBACK_POLL)
        end
    end
end

local function player_check_loop()
    if not station_config.player_detector then
        while true do sleep(60) end
    end
    while true do
        check_players_nearby()
        os.sleep(PLAYER_CHECK_INTERVAL)
    end
end

local function terminal_input()
    print("")
    print("Commands: name, hub, remote, setup, status, help")
    print("")
    while true do
        write("> ")
        local input = read()
        if input then
            local parts = {}
            for word in input:gmatch("%S+") do
                table.insert(parts, word)
            end
            local cmd = parts[1]

            if cmd == "name" then
                local name = table.concat(parts, " ", 2)
                if name ~= "" then
                    station_config.label = name
                    save_config()
                    print("Renamed to: " .. name)
                else
                    print("Current: " .. station_config.label)
                    write("New name: ")
                    local new_name = read()
                    if new_name and new_name ~= "" then
                        station_config.label = new_name
                        save_config()
                        print("Renamed to: " .. new_name)
                    end
                end

            elseif cmd == "hub" then
                station_config.is_hub = true
                HUB_ID = os.getComputerID()
                save_config()
                print("Set as HUB. Restart recommended.")

            elseif cmd == "remote" then
                station_config.is_hub = false
                HUB_ID = nil
                save_config()
                print("Set as REMOTE. Restart recommended.")

            elseif cmd == "setup" then
                run_setup()

            elseif cmd == "status" then
                print("Station:  " .. station_config.label)
                print("Mode:     " .. (station_config.is_hub and "HUB" or "REMOTE"))
                print("Train:    " .. (has_train and "YES" or "NO"))
                print("Hub ID:   " .. (HUB_ID and ("#" .. HUB_ID) or "NONE"))
                print("Integrs:  " .. #redstone_integrators)
                print("Switches: " .. #station_config.switches)

            elseif cmd == "rescan" then
                scan_integrators()
                scan_player_detectors()
                print("Found " .. #redstone_integrators .. " integrators, " .. #player_detectors .. " player detectors")

            elseif cmd == "help" then
                print("Commands:")
                print("  name [text]  - rename station")
                print("  hub          - set as hub station")
                print("  remote       - set as remote station")
                print("  setup        - run full setup wizard")
                print("  status       - show current status")
                print("  rescan       - rescan peripherals")
                print("  help         - show this help")

            elseif cmd and cmd ~= "" then
                print("Unknown: " .. cmd .. " (type 'help')")
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
    discovery_loop,
    train_arrival_handler,
    departure_handler,
    monitor_loop,
    monitor_touch_loop,
    detector_loop,
    player_check_loop,
    terminal_input,
    update_checker
)
