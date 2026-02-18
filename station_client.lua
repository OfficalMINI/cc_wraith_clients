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
local AUTO_RETURN_DELAY = 30     -- seconds before idle remote train returns to hub
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
    buffer_chest = nil,        -- peripheral name of trapped chest for logistics buffer (hub only)
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
    local ok, content = pcall(textutils.serialise, station_config)
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
-- Wraith OS transport service ID (discovered via rednet)
local WRAITH_ID = nil

if station_config.is_hub then
    HUB_ID = os.getComputerID()
end

-- ========================================
-- Modem Setup
-- ========================================
local function find_wireless_modem()
    -- Only look for wireless (ender) modems — never use wired modems for rednet
    for _, side in ipairs({"back", "top", "left", "right", "bottom", "front"}) do
        if peripheral.getType(side) == "modem" then
            local m = peripheral.wrap(side)
            if m and m.isWireless and m.isWireless() then
                return side
            end
        end
    end
    return nil
end

-- List all modems for diagnostics
print("Modems attached:")
for _, side in ipairs({"back", "top", "left", "right", "bottom", "front"}) do
    if peripheral.getType(side) == "modem" then
        local m = peripheral.wrap(side)
        local wireless = m and m.isWireless and m.isWireless()
        local already_open = rednet.isOpen(side)
        print("  " .. side .. ": " .. (wireless and "WIRELESS" or "wired") .. (already_open and " [already open]" or ""))
    end
end

local modem_side = find_wireless_modem()
if not modem_side then
    printError("No wireless/ender modem found!")
    printError("Attach an ender modem directly to this computer.")
    return
end
rednet.open(modem_side)
print("Rednet opened on: " .. modem_side .. " (wireless)")

-- Hub: register as discoverable service via rednet DNS
if station_config.is_hub then
    rednet.host(PROTOCOLS.ping, "station_hub")
    print("Hosting station_hub service")
end

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
local switches_locked = false     -- hub: true while train is in transit (switches held in bypass)
local switches_locked_for = nil   -- hub: destination station id we're waiting on
local switches_locked_time = 0    -- os.clock() when locked (for safety timeout)
local SWITCH_LOCK_TIMEOUT = 120   -- max seconds to hold switches locked
local pending_switch_lock = nil   -- {station_id, label} lock to engage when train departs hub
local train_enroute = nil         -- {from_label, to_label} tracks train in transit for display

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

local DISPATCH_DEPART_TIMEOUT = 30  -- safety timeout waiting for train to cross detector

local function dispatch()
    local ri = get_integrator(station_config.rail_periph)
    if not ri then
        print("ERROR: Rail integrator not found: " .. tostring(station_config.rail_periph))
        return
    end
    print(string.format("Dispatching: %s:%s -> ON (held until departure)",
        station_config.rail_periph, station_config.rail_face))
    pcall(ri.setOutput, station_config.rail_face, true)
    -- Keep rail powered until detector confirms train has actually left
    local timeout = os.startTimer(DISPATCH_DEPART_TIMEOUT)
    while true do
        local e, p1 = os.pullEvent()
        if e == "train_departed" then
            print("Train crossed detector, braking rail")
            break
        elseif e == "timer" and p1 == timeout then
            print("WARN: Dispatch timeout (" .. DISPATCH_DEPART_TIMEOUT .. "s), braking rail")
            break
        end
    end
    pcall(ri.setOutput, station_config.rail_face, false)
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
    print(string.format("Bay %d dispatch: %s:%s -> ON (held until departure)", sw_idx, sw.bay_rail_periph, sw.bay_rail_face))
    pcall(ri.setOutput, sw.bay_rail_face, true)
    -- Keep bay rail powered until bay detector confirms train has left
    local timeout = os.startTimer(DISPATCH_DEPART_TIMEOUT)
    while true do
        local e, p1 = os.pullEvent()
        if e == "bay_departed" and p1 == sw_idx then
            print("Bay " .. sw_idx .. " train crossed detector, braking rail")
            break
        elseif e == "timer" and p1 == timeout then
            print("WARN: Bay " .. sw_idx .. " dispatch timeout (" .. DISPATCH_DEPART_TIMEOUT .. "s), braking rail")
            break
        end
    end
    pcall(ri.setOutput, sw.bay_rail_face, false)
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
    if switches_locked then
        print("[auto-park] Switches locked (train in transit), skipping")
        return
    end

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
                -- Clear en-route status
                train_enroute = nil
                -- Hub: train arrived — unlock switches (inbound train reached hub safely)
                if station_config.is_hub and (switches_locked or pending_switch_lock) then
                    switches_locked = false
                    switches_locked_for = nil
                    pending_switch_lock = nil
                    print("[hub] Switches unlocked - train at hub")
                end
                os.queueEvent("train_arrived")
            else
                print("[det] >>> TRAIN DEPARTED <<<")
                os.queueEvent("train_departed")
                -- Hub: engage switch lock now that train has actually left
                if station_config.is_hub and pending_switch_lock then
                    switches_locked = true
                    switches_locked_for = pending_switch_lock.station_id
                    switches_locked_time = os.clock()
                    print("[hub] Switches locked - train en route to " .. (pending_switch_lock.label or "?"))
                    pending_switch_lock = nil
                end
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
                os.queueEvent("bay_departed", sw_idx)
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
local monitor_mode = "main"  -- "main", "config", "pick_integrator", "pick_face", "switch_face", "switch_parking", "pick_player_det", "pick_buffer_chest", "edit_switch", "schedules", "sched_new_type", "sched_pick_station", "sched_pick_items", "sched_set_amounts", "sched_pick_period", "sched_detail", "sched_confirm_delete"
local config_purpose = nil   -- "rail", "detector", "switch", "bay_detector", "bay_rail", "edit_sw_integrator", "edit_sw_bay_detector", "edit_sw_bay_rail"
local pending_switch = nil   -- temp switch being built
local editing_switch_idx = nil  -- index of switch being edited (nil = adding new)
local monitor_buttons = {}   -- rebuilt each render

local function mon_btn(y1, y2, action, data)
    table.insert(monitor_buttons, {y1 = y1, y2 = y2 or y1, action = action, data = data})
end

-- ========================================
-- Schedule UI State
-- ========================================
local cached_allow_list = {}    -- from Wraith: {{item, display_name, min_keep}, ...}
local cached_schedules = {}     -- from Wraith: schedule array for this station
local sched_scroll = 0
local sched_item_scroll = 0
local new_schedule = nil        -- temp table during creation flow
local sched_detail_idx = nil    -- which schedule is being viewed
local sched_status_msg = nil
local sched_status_time = 0

local PERIOD_PRESETS = {
    {label = "Manual",  seconds = 0},
    {label = "5 min",   seconds = 300},
    {label = "15 min",  seconds = 900},
    {label = "30 min",  seconds = 1800},
    {label = "Hourly",  seconds = 3600},
    {label = "4 Hours", seconds = 14400},
    {label = "Daily",   seconds = 86400},
}

local function format_period(seconds)
    if not seconds or seconds <= 0 then return "Manual" end
    if seconds < 60 then return seconds .. "s" end
    if seconds < 3600 then return math.floor(seconds / 60) .. "m" end
    if seconds < 86400 then return math.floor(seconds / 3600) .. "h" end
    return math.floor(seconds / 86400) .. "d"
end

local function sched_send(msg)
    if not WRAITH_ID then return end
    msg.station_id = os.getComputerID()
    if station_config.is_hub then msg.all = true end
    rednet.send(WRAITH_ID, msg, PROTOCOLS.command)
end

local function sched_fetch_data()
    if not WRAITH_ID then return end
    sched_send({action = "get_allow_list"})
    sched_send({action = "get_schedules"})
end

local function set_sched_status(text)
    sched_status_msg = text
    sched_status_time = os.clock()
end

local function clean_item_name(name)
    if not name then return "?" end
    local short = name:gsub("^%w+:", "")
    return short:gsub("_", " "):gsub("(%a)([%w]*)", function(a, b) return a:upper() .. b end)
end

-- ========================================
-- Network Map UI State
-- ========================================
local map_data = nil              -- cached {stations, hub_id, bay_summary, trip_stats}
local map_selected_station = nil  -- station ID tapped, nil = overview
local map_last_fetch = 0
local MAP_FETCH_INTERVAL = 10

local function map_fetch_data()
    if station_config.is_hub then
        -- Hub builds map data locally
        local stations_out = {}
        stations_out[os.getComputerID()] = {
            label = station_config.label,
            x = my_x, y = my_y, z = my_z,
            is_hub = true,
            online = true,
            has_train = has_train,
        }
        for id, st in pairs(connected_stations) do
            stations_out[id] = {
                label = st.label,
                x = st.x or 0, y = st.y or 0, z = st.z or 0,
                is_hub = false,
                online = st.online and (os.clock() - st.last_seen) < 15,
                has_train = st.has_train or false,
            }
        end
        local bay_total, bay_occ = 0, 0
        for si, sw in ipairs(station_config.switches) do
            if sw.parking then
                bay_total = bay_total + 1
                local bs = bay_states[si]
                if bs and bs.has_train then bay_occ = bay_occ + 1 end
            end
        end
        map_data = {
            stations = stations_out,
            hub_id = os.getComputerID(),
            bay_summary = {total = bay_total, occupied = bay_occ},
            trip_stats = {},
        }
        -- Also request from Wraith for trip stats
        if WRAITH_ID then
            sched_send({action = "get_network_status"})
        end
    elseif WRAITH_ID then
        sched_send({action = "get_network_status"})
    end
    map_last_fetch = os.clock()
end

local function compute_map_layout(stations, hub_id, mw, mh)
    local map_x1, map_y1 = 2, 3
    local map_x2, map_y2 = mw - 1, mh - 2
    local map_w = map_x2 - map_x1 + 1
    local map_h = map_y2 - map_y1 + 1

    local coords = {}
    local min_gx, max_gx = math.huge, -math.huge
    local min_gz, max_gz = math.huge, -math.huge
    local count = 0

    for id, st in pairs(stations) do
        local gx = st.x or 0
        local gz = st.z or 0
        table.insert(coords, {id = id, gx = gx, gz = gz, st = st})
        if gx < min_gx then min_gx = gx end
        if gx > max_gx then max_gx = gx end
        if gz < min_gz then min_gz = gz end
        if gz > max_gz then max_gz = gz end
        count = count + 1
    end

    if count == 0 then return {} end
    if count == 1 then
        local c = coords[1]
        return {{id = c.id, st = c.st, cx = math.floor(map_x1 + map_w / 2), cy = math.floor(map_y1 + map_h / 2)}}
    end

    local range_x = math.max(max_gx - min_gx, 10)
    local range_z = math.max(max_gz - min_gz, 10)

    local result = {}
    local used = {}

    for _, c in ipairs(coords) do
        local nx = (c.gx - min_gx) / range_x
        local nz = (c.gz - min_gz) / range_z
        local cx = math.floor(map_x1 + nx * (map_w - 1) + 0.5)
        local cy = math.floor(map_y1 + nz * (map_h - 1) + 0.5)
        cx = math.max(map_x1, math.min(map_x2, cx))
        cy = math.max(map_y1, math.min(map_y2, cy))

        local key = cx .. "," .. cy
        local attempts = 0
        local offsets = {{1,0},{-1,0},{0,1},{0,-1},{1,1},{-1,1},{1,-1},{-1,-1}}
        while used[key] and attempts < 8 do
            local off = offsets[(attempts % #offsets) + 1]
            cx = math.max(map_x1, math.min(map_x2, cx + off[1]))
            cy = math.max(map_y1, math.min(map_y2, cy + off[2]))
            key = cx .. "," .. cy
            attempts = attempts + 1
        end
        used[key] = true
        table.insert(result, {id = c.id, st = c.st, cx = cx, cy = cy})
    end
    return result
end

local function draw_map_line(mon, x1, y1, x2, y2, color)
    mon.setTextColor(color)
    local dx = x2 - x1
    local dy = y2 - y1
    local steps = math.max(math.abs(dx), math.abs(dy))
    if steps <= 1 then return end
    for i = 1, steps - 1 do
        local t = i / steps
        local x = math.floor(x1 + dx * t + 0.5)
        local y = math.floor(y1 + dy * t + 0.5)
        mon.setCursorPos(x, y)
        if math.abs(dy) < math.abs(dx) * 0.3 then
            mon.write("-")
        elseif math.abs(dx) < math.abs(dy) * 0.3 then
            mon.write("|")
        elseif (dx > 0) == (dy > 0) then
            mon.write("\\")
        else
            mon.write("/")
        end
    end
end

local function render_main_monitor()
    if not monitor then return end

    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    -- Title + MAP + SCHED + SETUP buttons
    monitor.setCursorPos(1, 1)
    monitor.setTextColor(colors.cyan)
    local title = station_config.label
    if station_config.is_hub then title = "[HUB] " .. title end
    local setup_lbl = "[SETUP]"
    local sched_lbl = "[SCHED]"
    local map_lbl = "[MAP]"
    monitor.write(title:sub(1, mw - #setup_lbl - #sched_lbl - #map_lbl - 2))

    local map_x = mw - #setup_lbl - #sched_lbl - #map_lbl
    monitor.setCursorPos(map_x, 1)
    monitor.setBackgroundColor(colors.green)
    monitor.setTextColor(colors.white)
    monitor.write(map_lbl)

    local sched_x = map_x + #map_lbl
    monitor.setCursorPos(sched_x, 1)
    monitor.setBackgroundColor(colors.purple)
    monitor.setTextColor(colors.white)
    monitor.write(sched_lbl)

    monitor.setCursorPos(mw - #setup_lbl + 1, 1)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.white)
    monitor.write(setup_lbl)
    monitor.setBackgroundColor(colors.black)
    mon_btn(1, 1, "open_map", {x1 = map_x, x2 = map_x + #map_lbl - 1})
    mon_btn(1, 1, "open_schedules", {x1 = sched_x, x2 = sched_x + #sched_lbl - 1})
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
    elseif train_enroute then
        monitor.setTextColor(colors.orange)
        local route_txt = train_enroute.from_label .. " > " .. train_enroute.to_label
        monitor.write(route_txt:sub(1, mw - 7))
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
        -- Edit button (tap switch row)
        mon_btn(cy, cy, "edit_switch", {idx = si, x1 = 1, x2 = mw - 4})
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

    -- Buffer Chest (Hub only)
    if station_config.is_hub then
        cy = cy + 1
        if cy <= mh then
            monitor.setCursorPos(1, cy)
            monitor.setTextColor(colors.white)
            monitor.write("Buffer Chest:")
            cy = cy + 1
            if cy <= mh then
                monitor.setCursorPos(2, cy)
                if station_config.buffer_chest then
                    monitor.setTextColor(colors.yellow)
                    monitor.write(station_config.buffer_chest:sub(1, mw - 12))
                    monitor.setCursorPos(mw - 2, cy)
                    monitor.setBackgroundColor(colors.red)
                    monitor.setTextColor(colors.white)
                    monitor.write("[X]")
                    monitor.setBackgroundColor(colors.black)
                    mon_btn(cy, cy, "remove_buffer", {x1 = mw - 2, x2 = mw})
                else
                    monitor.setTextColor(colors.gray)
                    monitor.write("NONE")
                    local set_lbl = "[SET]"
                    monitor.setCursorPos(mw - #set_lbl + 1, cy)
                    monitor.setBackgroundColor(colors.blue)
                    monitor.setTextColor(colors.white)
                    monitor.write(set_lbl)
                    monitor.setBackgroundColor(colors.black)
                    mon_btn(cy, cy, "pick_buffer", {x1 = mw - #set_lbl + 1, x2 = mw})
                end
            end
            cy = cy + 1
        end
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

local function render_edit_switch()
    if not monitor or not editing_switch_idx then return end
    local sw = station_config.switches[editing_switch_idx]
    if not sw then
        monitor_mode = "config"
        editing_switch_idx = nil
        return
    end

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
    monitor.setCursorPos(1, 2)
    monitor.setTextColor(colors.cyan)
    monitor.write("EDIT: " .. (sw.description or "Switch " .. editing_switch_idx))

    monitor.setCursorPos(1, 3)
    monitor.setTextColor(colors.gray)
    monitor.write(string.rep("-", mw))

    local cy = 4

    -- Switch integrator
    monitor.setCursorPos(1, cy)
    monitor.setTextColor(colors.white)
    monitor.write("Switch:")
    cy = cy + 1
    monitor.setCursorPos(2, cy)
    monitor.setTextColor(colors.yellow)
    local sw_lbl = (sw.peripheral_name or "?"):sub(1, mw - 12) .. ":" .. (sw.face or "?")
    monitor.write(sw_lbl)
    monitor.setCursorPos(mw - 4, cy)
    monitor.setBackgroundColor(colors.blue)
    monitor.setTextColor(colors.white)
    monitor.write("[CHG]")
    monitor.setBackgroundColor(colors.black)
    mon_btn(cy, cy, "edit_sw_change_integrator", {x1 = mw - 4, x2 = mw})
    cy = cy + 2

    -- Parking toggle
    monitor.setCursorPos(1, cy)
    monitor.setTextColor(colors.white)
    monitor.write("Parking: ")
    if sw.parking then
        monitor.setBackgroundColor(colors.green)
        monitor.write(" YES ")
    else
        monitor.setBackgroundColor(colors.gray)
        monitor.write(" NO  ")
    end
    monitor.setBackgroundColor(colors.black)
    monitor.write(" ")
    monitor.setBackgroundColor(colors.blue)
    monitor.write("[TOG]")
    monitor.setBackgroundColor(colors.black)
    mon_btn(cy, cy, "edit_sw_toggle_parking", {})
    cy = cy + 1

    -- Bay config (only if parking)
    if sw.parking then
        cy = cy + 1
        monitor.setCursorPos(1, cy)
        monitor.setTextColor(colors.white)
        monitor.write("Bay Detector:")
        cy = cy + 1
        monitor.setCursorPos(2, cy)
        monitor.setTextColor(colors.yellow)
        local bd_lbl = (sw.bay_detector_periph or "NOT SET"):sub(1, mw - 12) .. ":" .. (sw.bay_detector_face or "?")
        monitor.write(bd_lbl)
        monitor.setCursorPos(mw - 4, cy)
        monitor.setBackgroundColor(colors.blue)
        monitor.setTextColor(colors.white)
        monitor.write("[CHG]")
        monitor.setBackgroundColor(colors.black)
        mon_btn(cy, cy, "edit_sw_change_bay_det", {x1 = mw - 4, x2 = mw})
        cy = cy + 2

        monitor.setCursorPos(1, cy)
        monitor.setTextColor(colors.white)
        monitor.write("Bay Rail:")
        cy = cy + 1
        monitor.setCursorPos(2, cy)
        monitor.setTextColor(colors.yellow)
        local br_lbl = (sw.bay_rail_periph or "NOT SET"):sub(1, mw - 12) .. ":" .. (sw.bay_rail_face or "?")
        monitor.write(br_lbl)
        monitor.setCursorPos(mw - 4, cy)
        monitor.setBackgroundColor(colors.blue)
        monitor.setTextColor(colors.white)
        monitor.write("[CHG]")
        monitor.setBackgroundColor(colors.black)
        mon_btn(cy, cy, "edit_sw_change_bay_rail", {x1 = mw - 4, x2 = mw})
        cy = cy + 2
    end

    -- Delete button
    cy = cy + 1
    if cy <= mh then
        monitor.setCursorPos(1, cy)
        monitor.setBackgroundColor(colors.red)
        monitor.setTextColor(colors.white)
        monitor.write(" DELETE SWITCH ")
        monitor.setBackgroundColor(colors.black)
        mon_btn(cy, cy, "remove_switch", {idx = editing_switch_idx, x1 = 1, x2 = 14})
    end
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

local function render_buffer_chest_picker()
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
    monitor.write("SELECT BUFFER CHEST")

    monitor.setCursorPos(1, 3)
    monitor.setTextColor(colors.gray)
    monitor.write(string.rep("-", mw))

    -- Find trapped chests on the wired network
    local trapped = {}
    for _, name in ipairs(peripheral.getNames()) do
        local ok, ptype = pcall(peripheral.getType, name)
        if ok and ptype == "minecraft:trapped_chest" then
            table.insert(trapped, name)
        end
    end

    local cy = 4
    if #trapped == 0 then
        monitor.setCursorPos(2, cy)
        monitor.setTextColor(colors.red)
        monitor.write("No trapped chests found!")
    else
        for i, name in ipairs(trapped) do
            if cy > mh then break end
            local is_current = (name == station_config.buffer_chest)
            monitor.setCursorPos(1, cy)
            if is_current then
                monitor.setBackgroundColor(colors.blue)
            else
                monitor.setBackgroundColor(colors.gray)
            end
            monitor.setTextColor(colors.white)
            local entry = string.format(" %d. %s %s", i, name:sub(1, mw - 8), is_current and "*" or " ")
            monitor.write(entry .. string.rep(" ", math.max(0, mw - #entry)))
            monitor.setBackgroundColor(colors.black)
            mon_btn(cy, cy, "select_buffer", {name = name})
            cy = cy + 1
        end
    end
end

-- ========================================
-- Schedule Monitor Screens
-- ========================================

local function render_sched_header(title, mw, show_action, action_label, action_name)
    monitor.setCursorPos(1, 1)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.white)
    monitor.clearLine()
    monitor.write(" < BACK")
    mon_btn(1, 1, "sched_back", {x1 = 1, x2 = 7})
    if show_action and action_label then
        local ax = mw - #action_label
        monitor.setCursorPos(ax, 1)
        monitor.setBackgroundColor(colors.lime)
        monitor.setTextColor(colors.black)
        monitor.write(action_label)
        mon_btn(1, 1, action_name, {x1 = ax, x2 = mw})
    end
    monitor.setBackgroundColor(colors.black)
    monitor.setCursorPos(1, 2)
    monitor.setTextColor(colors.cyan)
    monitor.write(" " .. title)
end

local function render_schedules()
    if not monitor then return end
    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    render_sched_header("SCHEDULES (" .. #cached_schedules .. ")", mw, true, " + NEW ", "sched_new")

    -- Status message (fades after 5s)
    local status_y = 3
    if sched_status_msg and (os.clock() - sched_status_time) < 5 then
        monitor.setCursorPos(1, status_y)
        monitor.setTextColor(colors.yellow)
        monitor.write(" " .. sched_status_msg:sub(1, mw - 2))
        status_y = 4
    end

    if not WRAITH_ID then
        monitor.setCursorPos(1, status_y + 1)
        monitor.setTextColor(colors.orange)
        monitor.write(" Not connected to Wraith")
        return
    end

    if #cached_schedules == 0 then
        monitor.setCursorPos(1, status_y + 1)
        monitor.setTextColor(colors.lightGray)
        monitor.write(" No schedules yet")
        monitor.setCursorPos(1, status_y + 2)
        monitor.write(" Tap [+ NEW] to create one")
        return
    end

    local row_start = status_y
    local max_rows = mh - row_start - 1
    for i = 1 + sched_scroll, math.min(#cached_schedules, sched_scroll + max_rows) do
        local s = cached_schedules[i]
        local y = row_start + (i - sched_scroll - 1)
        if y > mh - 1 then break end

        local icon = (s.type == "delivery") and ">" or "<"
        local typ = (s.type == "delivery") and "DELIV" or "COLCT"
        local per = format_period(s.period)
        local detail = ""
        if s.target_label then
            -- Hub view: show target station name
            detail = s.target_label:sub(1, 10)
        elseif s.type == "delivery" and s.items then
            detail = #s.items .. " item" .. (#s.items ~= 1 and "s" or "")
        end
        local status = s.enabled and "ON" or "OFF"
        local status_col = s.enabled and colors.lime or colors.red

        monitor.setCursorPos(1, y)
        monitor.setTextColor(colors.white)
        monitor.write(string.format(" %s %-5s %-6s %-10s", icon, typ, per, detail))
        monitor.setTextColor(status_col)
        local sx = mw - 6
        monitor.setCursorPos(sx, y)
        monitor.write(string.format("%-3s", status))
        monitor.setTextColor(colors.lightBlue)
        monitor.setCursorPos(mw - 2, y)
        monitor.write("[>]")
        mon_btn(y, y, "sched_view", {idx = i})
    end

    -- Scroll buttons
    if sched_scroll > 0 then
        monitor.setCursorPos(mw - 3, row_start)
        monitor.setTextColor(colors.yellow)
        monitor.write("[^]")
        mon_btn(row_start, row_start, "sched_scroll_up", {x1 = mw - 3, x2 = mw})
    end
    if sched_scroll + max_rows < #cached_schedules then
        monitor.setCursorPos(mw - 3, mh)
        monitor.setTextColor(colors.yellow)
        monitor.write("[v]")
        mon_btn(mh, mh, "sched_scroll_down", {x1 = mw - 3, x2 = mw})
    end
end

local function render_sched_new_type()
    if not monitor then return end
    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    render_sched_header("NEW SCHEDULE", mw, false)

    monitor.setCursorPos(1, 4)
    monitor.setTextColor(colors.lightGray)
    monitor.write(" Select type:")

    local d_desc = station_config.is_hub and "Send items to station" or "Send items here"
    local c_desc = station_config.is_hub and "Collect from station" or "Pick up items"

    -- Delivery button
    local y = 6
    monitor.setCursorPos(2, y)
    monitor.setBackgroundColor(colors.blue)
    monitor.setTextColor(colors.white)
    local dlbl = " DELIVERY  "
    monitor.write(dlbl)
    monitor.setBackgroundColor(colors.black)
    monitor.setCursorPos(2 + #dlbl + 1, y)
    monitor.setTextColor(colors.lightGray)
    monitor.write(d_desc)
    mon_btn(y, y, "sched_type_delivery", {x1 = 2, x2 = 2 + #dlbl - 1})

    -- Collection button
    y = 8
    monitor.setCursorPos(2, y)
    monitor.setBackgroundColor(colors.blue)
    monitor.setTextColor(colors.white)
    local clbl = " COLLECTION "
    monitor.write(clbl)
    monitor.setBackgroundColor(colors.black)
    monitor.setCursorPos(2 + #clbl + 1, y)
    monitor.setTextColor(colors.lightGray)
    monitor.write(c_desc)
    mon_btn(y, y, "sched_type_collection", {x1 = 2, x2 = 2 + #clbl - 1})
end

local function render_sched_pick_station()
    if not monitor then return end
    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    render_sched_header("SELECT STATION", mw, false)

    monitor.setCursorPos(1, 4)
    monitor.setTextColor(colors.lightGray)
    local hint = new_schedule and new_schedule.type == "delivery" and " Deliver items to:" or " Collect items from:"
    monitor.write(hint)

    local sorted = {}
    for id, st in pairs(connected_stations) do
        table.insert(sorted, {id = id, label = st.label or ("Station #" .. id)})
    end
    table.sort(sorted, function(a, b) return a.label < b.label end)

    local y = 6
    for _, st in ipairs(sorted) do
        if y > mh - 1 then break end
        monitor.setCursorPos(2, y)
        monitor.setBackgroundColor(colors.blue)
        monitor.setTextColor(colors.white)
        local btn_text = " " .. st.label:sub(1, mw - 4) .. string.rep(" ", math.max(0, mw - 4 - #st.label))
        monitor.write(btn_text)
        monitor.setBackgroundColor(colors.black)
        mon_btn(y, y, "sched_pick_station_select", {id = st.id, label = st.label, x1 = 2, x2 = mw - 1})
        y = y + 2
    end

    if #sorted == 0 then
        monitor.setCursorPos(2, 6)
        monitor.setTextColor(colors.gray)
        monitor.write("No stations connected")
    end
end

local function render_sched_pick_items()
    if not monitor then return end
    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    local chosen = new_schedule and new_schedule.items or {}
    local count = #chosen
    render_sched_header("SELECT ITEMS (" .. count .. ")", mw, count > 0, " DONE > ", "sched_items_done")

    if #cached_allow_list == 0 then
        monitor.setCursorPos(1, 4)
        monitor.setTextColor(colors.orange)
        monitor.write(" No items in allow list")
        monitor.setCursorPos(1, 5)
        monitor.setTextColor(colors.lightGray)
        monitor.write(" Add items in Wraith transport app")
        return
    end

    local row_start = 3
    local max_rows = mh - row_start
    for i = 1 + sched_item_scroll, math.min(#cached_allow_list, sched_item_scroll + max_rows) do
        local item = cached_allow_list[i]
        local y = row_start + (i - sched_item_scroll - 1)
        if y > mh then break end

        local selected = false
        for _, name in ipairs(chosen) do
            if name == item.item then selected = true; break end
        end

        monitor.setCursorPos(1, y)
        if selected then
            monitor.setTextColor(colors.lime)
            monitor.write(" [x] ")
        else
            monitor.setTextColor(colors.lightGray)
            monitor.write(" [ ] ")
        end
        monitor.setTextColor(colors.white)
        local display = item.display_name or clean_item_name(item.item)
        monitor.write(display:sub(1, mw - 6))
        mon_btn(y, y, "sched_toggle_item", {item = item.item})
    end

    -- Scroll
    if sched_item_scroll > 0 then
        monitor.setCursorPos(mw - 3, row_start)
        monitor.setTextColor(colors.yellow)
        monitor.write("[^]")
        mon_btn(row_start, row_start, "sched_item_scroll_up", {x1 = mw - 3, x2 = mw})
    end
    if sched_item_scroll + max_rows < #cached_allow_list then
        monitor.setCursorPos(mw - 3, mh)
        monitor.setTextColor(colors.yellow)
        monitor.write("[v]")
        mon_btn(mh, mh, "sched_item_scroll_down", {x1 = mw - 3, x2 = mw})
    end
end

local function render_sched_set_amounts()
    if not monitor then return end
    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    render_sched_header("SET AMOUNTS", mw, true, " DONE > ", "sched_amounts_done")

    local items = new_schedule and new_schedule.items or {}
    local amounts = new_schedule and new_schedule.amounts or {}

    for i, item_name in ipairs(items) do
        local y = 2 + i
        if y > mh then break end
        local amt = amounts[item_name] or 64
        local display = nil
        for _, al in ipairs(cached_allow_list) do
            if al.item == item_name then display = al.display_name; break end
        end
        display = display or clean_item_name(item_name)

        monitor.setCursorPos(1, y)
        monitor.setTextColor(colors.white)
        monitor.write(" " .. display:sub(1, mw - 16))

        local btn_x = mw - 12
        monitor.setCursorPos(btn_x, y)
        monitor.setBackgroundColor(colors.red)
        monitor.setTextColor(colors.white)
        monitor.write("[-]")
        mon_btn(y, y, "sched_amount_dec", {item = item_name, x1 = btn_x, x2 = btn_x + 2})

        monitor.setBackgroundColor(colors.black)
        monitor.setTextColor(colors.yellow)
        monitor.setCursorPos(btn_x + 4, y)
        monitor.write(string.format("%3d", amt))

        local px = btn_x + 8
        monitor.setCursorPos(px, y)
        monitor.setBackgroundColor(colors.lime)
        monitor.setTextColor(colors.black)
        monitor.write("[+]")
        mon_btn(y, y, "sched_amount_inc", {item = item_name, x1 = px, x2 = px + 2})
        monitor.setBackgroundColor(colors.black)
    end
end

local function render_sched_pick_period()
    if not monitor then return end
    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    render_sched_header("SELECT PERIOD", mw, false)

    for i, preset in ipairs(PERIOD_PRESETS) do
        local y = 2 + i
        if y > mh then break end
        monitor.setCursorPos(2, y)
        monitor.setBackgroundColor(colors.blue)
        monitor.setTextColor(colors.white)
        local lbl = " " .. preset.label .. " "
        monitor.write(lbl)
        monitor.setBackgroundColor(colors.black)
        mon_btn(y, y, "sched_select_period", {seconds = preset.seconds, x1 = 2, x2 = 2 + #lbl - 1})
    end
end

local function render_sched_detail()
    if not monitor then return end
    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    local sched = cached_schedules[sched_detail_idx]
    if not sched then
        render_sched_header("SCHEDULE NOT FOUND", mw, false)
        return
    end

    local typ_str = (sched.type == "delivery") and "DELIVERY" or "COLLECTION"
    render_sched_header("SCHEDULE #" .. sched_detail_idx .. " - " .. typ_str, mw, false)

    local y = 3

    -- Target station (hub view only)
    if sched.target_label then
        monitor.setCursorPos(1, y)
        monitor.setTextColor(colors.lightGray)
        monitor.write(" To: ")
        monitor.setTextColor(colors.cyan)
        monitor.write(sched.target_label)
        y = y + 1
    end

    -- Period
    monitor.setCursorPos(1, y)
    monitor.setTextColor(colors.lightGray)
    monitor.write(" Period: ")
    monitor.setTextColor(colors.white)
    monitor.write(format_period(sched.period))
    local chg_x = mw - 7
    monitor.setCursorPos(chg_x, y)
    monitor.setBackgroundColor(colors.blue)
    monitor.setTextColor(colors.white)
    monitor.write("[CHANGE]")
    mon_btn(y, y, "sched_change_period_start", {x1 = chg_x, x2 = mw})
    monitor.setBackgroundColor(colors.black)
    y = y + 1

    -- Items (delivery only)
    if sched.type == "delivery" and sched.items then
        monitor.setCursorPos(1, y)
        monitor.setTextColor(colors.lightGray)
        monitor.write(" Items: ")
        monitor.setTextColor(colors.white)
        local item_strs = {}
        for _, item_name in ipairs(sched.items) do
            local display = nil
            for _, al in ipairs(cached_allow_list) do
                if al.item == item_name then display = al.display_name; break end
            end
            display = display or clean_item_name(item_name)
            local amt = (sched.amounts and sched.amounts[item_name]) or 64
            table.insert(item_strs, display .. "(" .. amt .. ")")
        end
        local items_line = table.concat(item_strs, ", ")
        monitor.write(items_line:sub(1, mw - 9))
        y = y + 1
    end

    -- Status
    monitor.setCursorPos(1, y)
    monitor.setTextColor(colors.lightGray)
    monitor.write(" Status: ")
    if sched.enabled then
        monitor.setTextColor(colors.lime)
        monitor.write("ON")
    else
        monitor.setTextColor(colors.red)
        monitor.write("OFF")
    end
    local tog_x = mw - 7
    monitor.setCursorPos(tog_x, y)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.white)
    monitor.write("[TOGGLE]")
    mon_btn(y, y, "sched_toggle_detail", {x1 = tog_x, x2 = mw})
    monitor.setBackgroundColor(colors.black)
    y = y + 1

    -- Last run
    if sched.last_run and sched.last_run > 0 then
        monitor.setCursorPos(1, y)
        monitor.setTextColor(colors.lightGray)
        local now = math.floor(os.epoch("utc") / 1000)
        local ago = now - sched.last_run
        monitor.write(" Last run: " .. format_period(ago) .. " ago")
    end
    y = y + 2

    -- Action buttons
    if y <= mh then
        monitor.setCursorPos(2, y)
        monitor.setBackgroundColor(colors.lime)
        monitor.setTextColor(colors.black)
        monitor.write(" RUN NOW ")
        mon_btn(y, y, "sched_run_now", {idx = sched_detail_idx, x1 = 2, x2 = 10})

        local del_x = mw - 9
        monitor.setCursorPos(del_x, y)
        monitor.setBackgroundColor(colors.red)
        monitor.setTextColor(colors.white)
        monitor.write(" DELETE ")
        mon_btn(y, y, "sched_delete_start", {x1 = del_x, x2 = mw - 1})
        monitor.setBackgroundColor(colors.black)
    end
end

local function render_sched_confirm_delete()
    if not monitor then return end
    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    monitor.setCursorPos(1, 2)
    monitor.setTextColor(colors.red)
    monitor.write(" DELETE SCHEDULE #" .. (sched_detail_idx or "?") .. "?")

    monitor.setCursorPos(1, 4)
    monitor.setTextColor(colors.lightGray)
    monitor.write(" This cannot be undone.")

    local y = 6
    monitor.setCursorPos(2, y)
    monitor.setBackgroundColor(colors.red)
    monitor.setTextColor(colors.white)
    monitor.write(" YES, DELETE ")
    mon_btn(y, y, "sched_delete_confirm", {x1 = 2, x2 = 14})

    local cx = mw - 10
    monitor.setCursorPos(cx, y)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.white)
    monitor.write(" CANCEL ")
    mon_btn(y, y, "sched_delete_cancel", {x1 = cx, x2 = cx + 7})
    monitor.setBackgroundColor(colors.black)
end

local function render_map()
    if not monitor then return end
    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    -- Title bar
    monitor.setCursorPos(1, 1)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.white)
    monitor.write("< BACK")
    local title = "NETWORK MAP"
    monitor.setCursorPos(math.floor((mw - #title) / 2) + 1, 1)
    monitor.setTextColor(colors.cyan)
    monitor.write(title)
    for x = 7, mw do
        if x < math.floor((mw - #title) / 2) + 1 or x > math.floor((mw - #title) / 2) + #title then
            monitor.setCursorPos(x, 1)
            monitor.setTextColor(colors.gray)
            monitor.write(" ")
        end
    end
    monitor.setBackgroundColor(colors.black)
    mon_btn(1, 1, "map_back", {x1 = 1, x2 = 6})

    -- Status bar
    monitor.setCursorPos(1, 2)
    if not map_data or not map_data.stations then
        monitor.setTextColor(colors.orange)
        monitor.write(" Loading...")
        return
    end

    local st_count, online_count, train_count = 0, 0, 0
    for _, st in pairs(map_data.stations) do
        st_count = st_count + 1
        if st.online then online_count = online_count + 1 end
        if st.has_train then train_count = train_count + 1 end
    end
    monitor.setTextColor(colors.lightGray)
    local status = string.format(" %d stations  %d online  %d trains", st_count, online_count, train_count)
    if map_data.bay_summary and map_data.bay_summary.total > 0 then
        status = status .. string.format("  Bays:%d/%d", map_data.bay_summary.occupied, map_data.bay_summary.total)
    end
    monitor.write(status:sub(1, mw))

    -- Map area
    local layout = compute_map_layout(map_data.stations, map_data.hub_id, mw, mh)

    -- Find hub node for drawing lines
    local hub_node = nil
    for _, node in ipairs(layout) do
        if node.st.is_hub then hub_node = node; break end
    end

    -- Draw connection lines from hub to each station
    if hub_node then
        for _, node in ipairs(layout) do
            if node.id ~= hub_node.id then
                draw_map_line(monitor, hub_node.cx, hub_node.cy, node.cx, node.cy, colors.gray)
            end
        end
    end

    -- Draw station markers on top
    for _, node in ipairs(layout) do
        local st = node.st
        local marker_color
        if not st.online then
            marker_color = colors.red
        elseif st.is_hub then
            marker_color = colors.yellow
        elseif st.has_train then
            marker_color = colors.lime
        else
            marker_color = colors.lightBlue
        end

        monitor.setCursorPos(node.cx, node.cy)
        monitor.setTextColor(marker_color)
        monitor.write(st.is_hub and "\4" or "\7")

        -- Short label next to marker
        local short = (st.label or "?"):sub(1, 8)
        local lx = node.cx + 2
        if lx + #short - 1 > mw then lx = node.cx - #short - 1 end
        if lx >= 1 and lx + #short - 1 <= mw then
            monitor.setCursorPos(lx, node.cy)
            monitor.setTextColor(st.is_hub and colors.yellow or colors.white)
            monitor.write(short)
        end

        mon_btn(node.cy, node.cy, "map_select_station", {
            id = node.id,
            x1 = math.max(1, node.cx - 1),
            x2 = math.min(mw, node.cx + #short + 2),
        })
    end

    -- Bottom: en-route or legend
    monitor.setCursorPos(1, mh)
    if train_enroute then
        monitor.setTextColor(colors.orange)
        monitor.write(string.format(" \16 %s > %s", train_enroute.from_label:sub(1, 12), train_enroute.to_label:sub(1, 12)))
    else
        monitor.setTextColor(colors.yellow)
        monitor.write("\4")
        monitor.write("Hub ")
        monitor.setTextColor(colors.lime)
        monitor.write("\7")
        monitor.write("Train ")
        monitor.setTextColor(colors.lightBlue)
        monitor.write("\7")
        monitor.write("Empty ")
        monitor.setTextColor(colors.red)
        monitor.write("\7")
        monitor.write("Off")
    end
end

local function render_map_detail()
    if not monitor or not map_data then return end
    local mw, mh = monitor.getSize()

    -- Render map underneath first
    render_map()

    local sid = map_selected_station
    local st = map_data.stations and map_data.stations[sid]
    if not st then
        map_selected_station = nil
        return
    end

    -- Detail overlay on bottom rows
    local detail_h = math.min(7, mh - 3)
    local dy = mh - detail_h + 1

    for y = dy, mh do
        monitor.setCursorPos(1, y)
        monitor.setBackgroundColor(colors.gray)
        monitor.write(string.rep(" ", mw))
    end

    -- Close button
    monitor.setCursorPos(mw - 2, dy)
    monitor.setBackgroundColor(colors.red)
    monitor.setTextColor(colors.white)
    monitor.write("[X]")
    mon_btn(dy, dy, "map_close_detail", {x1 = mw - 2, x2 = mw})

    -- Station name
    monitor.setCursorPos(1, dy)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(st.is_hub and colors.yellow or colors.cyan)
    local name = (st.is_hub and "[HUB] " or "") .. (st.label or "?")
    monitor.write(name:sub(1, mw - 4))

    -- Status
    local cy = dy + 1
    monitor.setCursorPos(1, cy)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(st.online and colors.lime or colors.red)
    monitor.write(st.online and "Online" or "Offline")
    monitor.setTextColor(colors.white)
    monitor.write("  Train: ")
    monitor.setTextColor(st.has_train and colors.lime or colors.lightGray)
    monitor.write(st.has_train and "YES" or "NO")

    -- Coordinates
    cy = cy + 1
    monitor.setCursorPos(1, cy)
    monitor.setBackgroundColor(colors.gray)
    monitor.setTextColor(colors.lightGray)
    monitor.write(string.format("Pos: %d, %d, %d", st.x or 0, st.y or 0, st.z or 0))

    -- Trip stats
    cy = cy + 1
    monitor.setCursorPos(1, cy)
    monitor.setBackgroundColor(colors.gray)
    local stats = map_data.trip_stats and map_data.trip_stats[sid]
    if stats and stats.avg_duration_ms then
        local avg_sec = math.floor(stats.avg_duration_ms / 1000)
        monitor.setTextColor(colors.white)
        monitor.write("Avg trip: ")
        monitor.setTextColor(colors.yellow)
        if avg_sec < 60 then
            monitor.write(avg_sec .. "s")
        else
            monitor.write(math.floor(avg_sec / 60) .. "m " .. (avg_sec % 60) .. "s")
        end
        monitor.setTextColor(colors.lightGray)
        monitor.write(" (" .. (stats.trip_count or 0) .. " trips)")
    else
        monitor.setTextColor(colors.lightGray)
        monitor.write("No trip data")
    end

    -- Last trip
    if stats and stats.last_trip_time then
        cy = cy + 1
        monitor.setCursorPos(1, cy)
        monitor.setBackgroundColor(colors.gray)
        monitor.setTextColor(colors.lightGray)
        local ago = math.floor((os.epoch("utc") - stats.last_trip_time) / 1000)
        local ago_str
        if ago < 60 then ago_str = ago .. "s ago"
        elseif ago < 3600 then ago_str = math.floor(ago / 60) .. "m ago"
        else ago_str = math.floor(ago / 3600) .. "h ago" end
        monitor.write("Last trip: " .. ago_str)
    end

    -- GO TO button (not for self)
    if sid ~= os.getComputerID() then
        cy = cy + 1
        if cy <= mh then
            local go_lbl = " GO TO "
            local go_x = math.floor((mw - #go_lbl) / 2) + 1
            monitor.setCursorPos(go_x, cy)
            monitor.setBackgroundColor(colors.blue)
            monitor.setTextColor(colors.white)
            monitor.write(go_lbl)
            mon_btn(cy, cy, "dispatch_to", {id = sid, label = st.label, x1 = go_x, x2 = go_x + #go_lbl - 1})
        end
    end
    monitor.setBackgroundColor(colors.black)
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
    elseif monitor_mode == "edit_switch" then
        render_edit_switch()
    elseif monitor_mode == "pick_player_det" then
        render_player_detector_picker()
    elseif monitor_mode == "pick_buffer_chest" then
        render_buffer_chest_picker()
    elseif monitor_mode == "schedules" then
        render_schedules()
    elseif monitor_mode == "sched_new_type" then
        render_sched_new_type()
    elseif monitor_mode == "sched_pick_station" then
        render_sched_pick_station()
    elseif monitor_mode == "sched_pick_items" then
        render_sched_pick_items()
    elseif monitor_mode == "sched_set_amounts" then
        render_sched_set_amounts()
    elseif monitor_mode == "sched_pick_period" then
        render_sched_pick_period()
    elseif monitor_mode == "sched_detail" then
        render_sched_detail()
    elseif monitor_mode == "sched_confirm_delete" then
        render_sched_confirm_delete()
    elseif monitor_mode == "map" then
        render_map()
    elseif monitor_mode == "map_detail" then
        render_map_detail()
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

    -- Method 1: rednet.lookup (CC:Tweaked DNS — most reliable)
    print("[discovery] Looking up station_hub...")
    local hub_id = rednet.lookup(PROTOCOLS.ping, "station_hub")
    if hub_id then
        HUB_ID = hub_id
        print("[discovery] Found hub #" .. hub_id .. " via lookup")
        -- Request station list
        rednet.send(HUB_ID, {
            type = "station_ping",
            label = station_config.label,
            id = os.getComputerID(),
        }, PROTOCOLS.ping)
        local sender, msg = rednet.receive(PROTOCOLS.status, DISCOVERY_TIMEOUT)
        if sender and type(msg) == "table" and msg.stations then
            route_data = msg.stations
        end
        return true
    end

    -- Method 2: fallback broadcast
    print("[discovery] Lookup failed, broadcasting ping...")
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
        print("[discovery] Found hub #" .. sender .. " via broadcast")
        return true
    end
    print("[discovery] No hub found")
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
-- Wraith OS Discovery & Registration
-- ========================================
local function discover_wraith()
    -- Look for Wraith's transport service via rednet DNS
    local wraith_id = rednet.lookup(PROTOCOLS.ping, "wraith_rail_hub")
    if wraith_id and wraith_id ~= os.getComputerID() then
        -- Don't connect to self, and don't confuse with hub station
        if wraith_id ~= HUB_ID then
            WRAITH_ID = wraith_id
            print("[wraith] Found Wraith OS #" .. wraith_id)
            return true
        end
    end
    return false
end

local function register_with_wraith()
    if not WRAITH_ID then return false end
    rednet.send(WRAITH_ID, {
        label = station_config.label,
        id = os.getComputerID(),
        x = my_x, y = my_y, z = my_z,
        is_hub = station_config.is_hub,
        has_train = has_train,
        switches = station_config.switches,
        rail_periph = station_config.rail_periph,
        rail_face = station_config.rail_face,
        detector_periph = station_config.detector_periph,
        detector_face = station_config.detector_face,
    }, PROTOCOLS.register)
    local sender, msg = rednet.receive(PROTOCOLS.status, 3)
    if sender == WRAITH_ID and type(msg) == "table" then
        print("[wraith] Registered with Wraith OS #" .. WRAITH_ID)
        return true
    end
    return false
end

local function heartbeat_wraith()
    if not WRAITH_ID then return end
    rednet.send(WRAITH_ID, {
        id = os.getComputerID(),
        label = station_config.label,
        has_train = has_train,
        is_hub = station_config.is_hub,
    }, PROTOCOLS.heartbeat)
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
                print("[hub] Ping from #" .. sender .. " (" .. (msg.label or "?") .. ")")
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
                else
                    -- Unknown station sent heartbeat — auto-register it
                    connected_stations[sender] = {
                        id = sender,
                        label = msg.label or ("Station #" .. sender),
                        x = msg.x or 0, y = msg.y or 0, z = msg.z or 0,
                        has_train = msg.has_train or false,
                        switches = {},
                        online = true,
                        last_seen = os.clock(),
                    }
                    print("Station auto-registered: " .. (msg.label or "#" .. sender))
                end
                -- Unlock switches if destination station reports train arrived
                if switches_locked and msg.has_train == true then
                    if switches_locked_for == sender then
                        switches_locked = false
                        switches_locked_for = nil
                        pending_switch_lock = nil
                        train_enroute = nil
                        print("[hub] Train arrived at #" .. sender .. " - switches unlocked")
                    end
                end
                -- Send heartbeat response so remote knows hub is alive
                rednet.send(sender, {
                    status = "heartbeat_ack",
                    stations = get_station_list(),
                }, PROTOCOLS.status)

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
                    -- Transport service or remote station requesting a train
                    if station_config.is_hub and not switches_locked and not pending_outbound and not pending_destination then
                        local target_id = msg.target or msg.station_id or sender
                        local target_label = msg.target_label or msg.label or ("Station #" .. target_id)
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
                            if msg.trip_type == "delivery" then
                                -- Delivery: train must visit hub first to load items from buffer
                                -- Pull train from bay to hub, then dispatch onward to destination
                                pending_outbound = {station_id = target_id, label = target_label}
                                for i, sw in ipairs(station_config.switches) do
                                    if sw.parking then
                                        set_switch(i, i ~= bay_idx)
                                    end
                                end
                                print("[hub] Pulling bay " .. bay_idx .. " to hub for delivery -> " .. target_label)
                                dispatch_from_bay(bay_idx)
                            else
                                -- Collection/other: send empty train direct from bay to destination
                                for i, sw in ipairs(station_config.switches) do
                                    if sw.parking then
                                        set_switch(i, true)
                                    end
                                end
                                switches_locked = true
                                switches_locked_for = target_id
                                switches_locked_time = os.clock()
                                train_enroute = {from_label = station_config.label, to_label = target_label}
                                print("[hub] Direct dispatch bay " .. bay_idx .. " -> " .. target_label)
                                dispatch_from_bay(bay_idx)
                            end
                        else
                            print("[hub] No trains available for " .. target_label)
                            rednet.send(sender, {action = "train_unavailable"}, PROTOCOLS.command)
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

                -- Schedule data responses from Wraith
                elseif msg.action == "allow_list" then
                    if msg.items then
                        cached_allow_list = msg.items
                        print("[sched] Allow list: " .. #cached_allow_list .. " items")
                        render_monitor()
                    end

                elseif msg.action == "schedules" then
                    if msg.schedules then
                        cached_schedules = msg.schedules
                        print("[sched] Schedules: " .. #cached_schedules)
                        render_monitor()
                    end

                elseif msg.action == "schedule_ok" then
                    set_sched_status(msg.message or "OK")
                    render_monitor()

                elseif msg.action == "schedule_error" then
                    set_sched_status("ERR: " .. (msg.message or "?"))
                    render_monitor()

                elseif msg.action == "schedule_run_ok" then
                    set_sched_status(msg.message or "Trip started")
                    render_monitor()

                elseif msg.action == "schedule_run_error" then
                    set_sched_status("ERR: " .. (msg.message or "?"))
                    render_monitor()

                -- Network map data response
                elseif msg.action == "network_status" then
                    if station_config.is_hub and map_data then
                        -- Hub: merge trip_stats from Wraith into local map_data
                        map_data.trip_stats = msg.trip_stats or {}
                        if msg.bay_summary then map_data.bay_summary = msg.bay_summary end
                    else
                        map_data = {
                            stations = msg.stations or {},
                            hub_id = msg.hub_id,
                            bay_summary = msg.bay_summary or {total = 0, occupied = 0},
                            trip_stats = msg.trip_stats or {},
                        }
                    end
                    render_monitor()

                elseif msg.action == "request_dispatch" and station_config.is_hub then
                    -- Remote station is sending a train TO hub — set switches OFF for inbound
                    if msg.to == os.getComputerID() then
                        -- Unlock any existing lock
                        switches_locked = false
                        switches_locked_for = nil
                        pending_switch_lock = nil
                        -- Set all parking switches OFF so train can reach hub
                        for i, sw in ipairs(station_config.switches) do
                            if sw.parking then
                                set_switch(i, false)
                            end
                        end
                        local from_lbl = msg.from_label or (msg.from and ("#" .. msg.from) or "?")
                        train_enroute = {from_label = from_lbl, to_label = station_config.label}
                        print("[hub] Inbound train from " .. from_lbl .. " - switches open")
                    end
                end
            end
        end
    end
end

local function discovery_loop()
    -- Wraith discovery counter — try every 3rd loop iteration
    local wraith_tick = 0

    if station_config.is_hub then
        -- Hub: check for offline stations + switch lock timeout + Wraith heartbeat
        -- Discover Wraith on startup
        if not WRAITH_ID then
            discover_wraith()
            if WRAITH_ID then register_with_wraith() end
        end

        while true do
            sleep(10)
            local now = os.clock()
            for id, st in pairs(connected_stations) do
                if st.online and st.last_seen > 0 and (now - st.last_seen) > 30 then
                    st.online = false
                    print("Station offline: " .. (st.label or "#" .. id))
                end
            end
            -- Safety: unlock switches if timeout exceeded
            if switches_locked and (now - switches_locked_time) > SWITCH_LOCK_TIMEOUT then
                print("[hub] Switch lock timeout (" .. SWITCH_LOCK_TIMEOUT .. "s) - unlocking")
                switches_locked = false
                switches_locked_for = nil
            end
            -- Heartbeat to Wraith OS
            wraith_tick = wraith_tick + 1
            if WRAITH_ID then
                heartbeat_wraith()
            elseif wraith_tick % 3 == 0 then
                -- Periodically retry Wraith discovery
                discover_wraith()
                if WRAITH_ID then register_with_wraith() end
            end
        end
    else
        -- Remote: discover and maintain connection to hub + Wraith
        local missed_pings = 0

        -- Discover Wraith on startup
        if not WRAITH_ID then
            discover_wraith()
            if WRAITH_ID then register_with_wraith() end
        end

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
            -- Heartbeat to Wraith OS
            wraith_tick = wraith_tick + 1
            if WRAITH_ID then
                heartbeat_wraith()
            elseif wraith_tick % 3 == 0 then
                discover_wraith()
                if WRAITH_ID then register_with_wraith() end
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

            -- Train is now at hub — set ALL switches to bypass (ON) for outbound
            for i, sw in ipairs(station_config.switches) do
                if sw.parking then
                    set_switch(i, true)
                end
            end
            -- Prepare lock — will engage when hub detector confirms train departed
            pending_switch_lock = {
                station_id = target.station_id,
                label = target.label or ("#" .. target.station_id),
            }

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

        elseif not station_config.is_hub and HUB_ID then
            -- Remote: auto-return idle train to hub after delay
            print("[auto-return] Train arrived, waiting " .. AUTO_RETURN_DELAY .. "s...")
            local return_timer = os.startTimer(AUTO_RETURN_DELAY)
            local should_return = true
            while true do
                local e, p1 = os.pullEvent()
                if e == "timer" and p1 == return_timer then
                    break
                elseif e == "train_departed" then
                    print("[auto-return] Cancelled (train left)")
                    should_return = false
                    break
                elseif e == "destination_selected" then
                    print("[auto-return] Cancelled (destination selected)")
                    should_return = false
                    break
                end
            end
            -- Wait for players to leave before returning
            if should_return and has_train then
                check_players_nearby()
                if players_nearby then
                    print("[auto-return] Players nearby, waiting...")
                    while true do
                        if not has_train then
                            should_return = false
                            break
                        end
                        check_players_nearby()
                        if not players_nearby then
                            print("[auto-return] Players left, returning to hub...")
                            break
                        end
                        os.sleep(PLAYER_CHECK_INTERVAL)
                    end
                end
                if should_return and has_train then
                    print("[auto-return] Sending train back to hub #" .. HUB_ID)
                    -- Notify hub so it sets switches OFF for inbound
                    rednet.send(HUB_ID, {
                        action = "request_dispatch",
                        from = os.getComputerID(),
                        from_label = station_config.label,
                        to = HUB_ID,
                    }, PROTOCOLS.command)
                    os.sleep(1)
                    dispatch()
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
            -- Hub: set switches to bypass parking bays, lock when train departs
            if station_config.is_hub then
                for i, sw in ipairs(station_config.switches) do
                    if sw.parking then
                        set_switch(i, true)
                    end
                end
                pending_switch_lock = {
                    station_id = dest.id,
                    label = dest.label,
                }
            end
            -- Notify hub about outbound dispatch (remote stations)
            if HUB_ID and HUB_ID ~= os.getComputerID() then
                rednet.send(HUB_ID, {
                    action = "request_dispatch",
                    from = os.getComputerID(),
                    to = dest.id,
                }, PROTOCOLS.command)
            end
            train_enroute = {from_label = station_config.label, to_label = dest.label}
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
        -- Auto-refresh map data when viewing map
        if (monitor_mode == "map" or monitor_mode == "map_detail")
            and (os.clock() - map_last_fetch) > MAP_FETCH_INTERVAL then
            map_fetch_data()
        end
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
                    if editing_switch_idx and (config_purpose or ""):find("^edit_sw") then
                        -- Return to edit switch screen when backing out of edit sub-flow
                        monitor_mode = "edit_switch"
                    else
                        monitor_mode = "config"
                        editing_switch_idx = nil
                    end
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
                                    -- Target bay switch OFF (open path out), others ON (block)
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
                    elseif config_purpose == "edit_sw_integrator" then
                        local sw = station_config.switches[editing_switch_idx]
                        if sw then sw.peripheral_name = btn.data.name end
                        monitor_mode = "pick_face"
                    elseif config_purpose == "edit_sw_bay_detector" then
                        local sw = station_config.switches[editing_switch_idx]
                        if sw then sw.bay_detector_periph = btn.data.name end
                        monitor_mode = "pick_face"
                    elseif config_purpose == "edit_sw_bay_rail" then
                        local sw = station_config.switches[editing_switch_idx]
                        if sw then sw.bay_rail_periph = btn.data.name end
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
                    elseif config_purpose == "edit_sw_integrator" then
                        local sw = station_config.switches[editing_switch_idx]
                        if sw then
                            sw.face = btn.data.face
                            save_config()
                        end
                        config_purpose = nil
                        monitor_mode = "edit_switch"
                    elseif config_purpose == "edit_sw_bay_detector" then
                        local sw = station_config.switches[editing_switch_idx]
                        if sw then
                            sw.bay_detector_face = btn.data.face
                            save_config()
                        end
                        config_purpose = nil
                        monitor_mode = "edit_switch"
                    elseif config_purpose == "edit_sw_bay_rail" then
                        local sw = station_config.switches[editing_switch_idx]
                        if sw then
                            sw.bay_rail_face = btn.data.face
                            save_config()
                        end
                        config_purpose = nil
                        monitor_mode = "edit_switch"
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
                    editing_switch_idx = nil
                    monitor_mode = "config"
                    render_monitor()

                elseif btn.action == "edit_switch" then
                    editing_switch_idx = btn.data.idx
                    monitor_mode = "edit_switch"
                    render_monitor()

                elseif btn.action == "edit_sw_change_integrator" then
                    config_purpose = "edit_sw_integrator"
                    monitor_mode = "pick_integrator"
                    render_monitor()

                elseif btn.action == "edit_sw_toggle_parking" then
                    local sw = station_config.switches[editing_switch_idx]
                    if sw then
                        sw.parking = not sw.parking
                        if not sw.parking then
                            sw.bay_detector_periph = nil
                            sw.bay_detector_face = nil
                            sw.bay_rail_periph = nil
                            sw.bay_rail_face = nil
                            sw.bay_has_train = false
                            bay_states[editing_switch_idx] = nil
                        else
                            bay_states[editing_switch_idx] = {
                                last_signal = false,
                                last_toggle_time = 0,
                                has_train = sw.bay_has_train or false,
                            }
                        end
                        save_config()
                    end
                    render_monitor()

                elseif btn.action == "edit_sw_change_bay_det" then
                    config_purpose = "edit_sw_bay_detector"
                    monitor_mode = "pick_integrator"
                    render_monitor()

                elseif btn.action == "edit_sw_change_bay_rail" then
                    config_purpose = "edit_sw_bay_rail"
                    monitor_mode = "pick_integrator"
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

                elseif btn.action == "pick_buffer" then
                    monitor_mode = "pick_buffer_chest"
                    render_monitor()

                elseif btn.action == "select_buffer" then
                    station_config.buffer_chest = btn.data.name
                    save_config()
                    print("Buffer chest set: " .. btn.data.name)
                    monitor_mode = "config"
                    render_monitor()

                elseif btn.action == "remove_buffer" then
                    station_config.buffer_chest = nil
                    save_config()
                    print("Buffer chest removed")
                    render_monitor()

                elseif btn.action == "rescan" then
                    scan_integrators()
                    scan_player_detectors()
                    print("Rescanned: " .. #redstone_integrators .. " integrators, " .. #player_detectors .. " player detectors")
                    render_monitor()

                -- ==============================
                -- Map actions
                -- ==============================
                elseif btn.action == "open_map" then
                    map_selected_station = nil
                    map_fetch_data()
                    monitor_mode = "map"
                    render_monitor()

                elseif btn.action == "map_back" then
                    monitor_mode = "main"
                    map_selected_station = nil
                    render_monitor()

                elseif btn.action == "map_select_station" then
                    if btn.data and btn.data.id then
                        map_selected_station = btn.data.id
                        monitor_mode = "map_detail"
                        render_monitor()
                    end

                elseif btn.action == "map_close_detail" then
                    map_selected_station = nil
                    monitor_mode = "map"
                    render_monitor()

                -- ==============================
                -- Schedule management actions
                -- ==============================
                elseif btn.action == "open_schedules" then
                    sched_scroll = 0
                    sched_fetch_data()
                    monitor_mode = "schedules"
                    render_monitor()

                elseif btn.action == "sched_back" then
                    if monitor_mode == "sched_new_type" then
                        new_schedule = nil
                        monitor_mode = "schedules"
                    elseif monitor_mode == "sched_pick_station" then
                        monitor_mode = "sched_new_type"
                    elseif monitor_mode == "sched_pick_items" then
                        if station_config.is_hub then
                            monitor_mode = "sched_pick_station"
                        else
                            monitor_mode = "sched_new_type"
                        end
                    elseif monitor_mode == "sched_set_amounts" then
                        monitor_mode = "sched_pick_items"
                    elseif monitor_mode == "sched_pick_period" then
                        if not new_schedule then
                            -- Changing period on existing schedule — go back to detail
                            monitor_mode = "sched_detail"
                        elseif new_schedule.type == "collection" then
                            if station_config.is_hub then
                                monitor_mode = "sched_pick_station"
                            else
                                monitor_mode = "sched_new_type"
                            end
                        else
                            monitor_mode = "sched_set_amounts"
                        end
                    elseif monitor_mode == "sched_detail" then
                        monitor_mode = "schedules"
                    elseif monitor_mode == "sched_confirm_delete" then
                        monitor_mode = "sched_detail"
                    else
                        monitor_mode = "main"
                    end
                    render_monitor()

                elseif btn.action == "sched_new" then
                    new_schedule = {type = nil, items = {}, amounts = {}, period = 3600}
                    sched_item_scroll = 0
                    monitor_mode = "sched_new_type"
                    render_monitor()

                elseif btn.action == "sched_type_delivery" then
                    new_schedule.type = "delivery"
                    sched_item_scroll = 0
                    if station_config.is_hub then
                        monitor_mode = "sched_pick_station"
                    else
                        monitor_mode = "sched_pick_items"
                    end
                    render_monitor()

                elseif btn.action == "sched_type_collection" then
                    new_schedule.type = "collection"
                    if station_config.is_hub then
                        monitor_mode = "sched_pick_station"
                    else
                        monitor_mode = "sched_pick_period"
                    end
                    render_monitor()

                elseif btn.action == "sched_pick_station_select" then
                    if new_schedule and btn.data then
                        new_schedule.target_id = btn.data.id
                        new_schedule.target_label = btn.data.label
                        if new_schedule.type == "delivery" then
                            monitor_mode = "sched_pick_items"
                        else
                            monitor_mode = "sched_pick_period"
                        end
                        render_monitor()
                    end

                elseif btn.action == "sched_toggle_item" then
                    if new_schedule and btn.data and btn.data.item then
                        local item_name = btn.data.item
                        local found = false
                        for j, name in ipairs(new_schedule.items) do
                            if name == item_name then
                                table.remove(new_schedule.items, j)
                                new_schedule.amounts[item_name] = nil
                                found = true
                                break
                            end
                        end
                        if not found then
                            table.insert(new_schedule.items, item_name)
                            new_schedule.amounts[item_name] = 64
                        end
                        render_monitor()
                    end

                elseif btn.action == "sched_items_done" then
                    if new_schedule and #new_schedule.items > 0 then
                        monitor_mode = "sched_set_amounts"
                    end
                    render_monitor()

                elseif btn.action == "sched_amount_dec" then
                    if new_schedule and btn.data and btn.data.item then
                        local amt = new_schedule.amounts[btn.data.item] or 64
                        amt = math.max(1, amt - 16)
                        new_schedule.amounts[btn.data.item] = amt
                        render_monitor()
                    end

                elseif btn.action == "sched_amount_inc" then
                    if new_schedule and btn.data and btn.data.item then
                        local amt = new_schedule.amounts[btn.data.item] or 64
                        amt = math.min(256, amt + 16)
                        new_schedule.amounts[btn.data.item] = amt
                        render_monitor()
                    end

                elseif btn.action == "sched_amounts_done" then
                    monitor_mode = "sched_pick_period"
                    render_monitor()

                elseif btn.action == "sched_select_period" then
                    if new_schedule then
                        -- Creating new schedule
                        new_schedule.period = btn.data.seconds
                        local cmd = {
                            action = "add_schedule",
                            schedule = {
                                type = new_schedule.type,
                                items = new_schedule.items,
                                amounts = new_schedule.amounts,
                                period = new_schedule.period,
                            },
                        }
                        if new_schedule.target_id then
                            cmd.target_id = new_schedule.target_id
                        end
                        sched_send(cmd)
                        set_sched_status("Saving...")
                        new_schedule = nil
                        monitor_mode = "schedules"
                    else
                        -- Updating period on existing schedule
                        local cmd = {action = "update_schedule", idx = sched_detail_idx, field = "period", value = btn.data.seconds}
                        -- Hub: include target_id from cached schedule
                        if station_config.is_hub and cached_schedules[sched_detail_idx] then
                            local s = cached_schedules[sched_detail_idx]
                            cmd.target_id = s.target_id
                            cmd.idx = s.orig_idx or sched_detail_idx
                        end
                        sched_send(cmd)
                        set_sched_status("Saving...")
                        monitor_mode = "sched_detail"
                    end
                    render_monitor()

                elseif btn.action == "sched_view" then
                    if btn.data and btn.data.idx then
                        sched_detail_idx = btn.data.idx
                        monitor_mode = "sched_detail"
                        render_monitor()
                    end

                elseif btn.action == "sched_toggle_detail" then
                    if sched_detail_idx and cached_schedules[sched_detail_idx] then
                        local s = cached_schedules[sched_detail_idx]
                        local cmd = {action = "toggle_schedule", idx = s.orig_idx or sched_detail_idx}
                        if s.target_id then cmd.target_id = s.target_id end
                        sched_send(cmd)
                        set_sched_status("Toggling...")
                        render_monitor()
                    end

                elseif btn.action == "sched_run_now" then
                    if btn.data and btn.data.idx and cached_schedules[btn.data.idx] then
                        local s = cached_schedules[btn.data.idx]
                        local cmd = {action = "run_schedule", idx = s.orig_idx or btn.data.idx}
                        if s.target_id then cmd.target_id = s.target_id end
                        sched_send(cmd)
                        set_sched_status("Running...")
                        render_monitor()
                    end

                elseif btn.action == "sched_change_period_start" then
                    -- Reuse period picker for editing (new_schedule is nil = edit mode)
                    new_schedule = nil
                    monitor_mode = "sched_pick_period"
                    render_monitor()

                elseif btn.action == "sched_delete_start" then
                    monitor_mode = "sched_confirm_delete"
                    render_monitor()

                elseif btn.action == "sched_delete_confirm" then
                    if sched_detail_idx and cached_schedules[sched_detail_idx] then
                        local s = cached_schedules[sched_detail_idx]
                        local cmd = {action = "remove_schedule", idx = s.orig_idx or sched_detail_idx}
                        if s.target_id then cmd.target_id = s.target_id end
                        sched_send(cmd)
                        set_sched_status("Deleting...")
                    end
                    sched_detail_idx = nil
                    monitor_mode = "schedules"
                    render_monitor()

                elseif btn.action == "sched_delete_cancel" then
                    monitor_mode = "sched_detail"
                    render_monitor()

                elseif btn.action == "sched_scroll_up" then
                    sched_scroll = math.max(0, sched_scroll - 1)
                    render_monitor()

                elseif btn.action == "sched_scroll_down" then
                    sched_scroll = sched_scroll + 1
                    render_monitor()

                elseif btn.action == "sched_item_scroll_up" then
                    sched_item_scroll = math.max(0, sched_item_scroll - 1)
                    render_monitor()

                elseif btn.action == "sched_item_scroll_down" then
                    sched_item_scroll = sched_item_scroll + 1
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
    print("Commands: name, hub, remote, connect, pos, test, status, help")
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
                rednet.host(PROTOCOLS.ping, "station_hub")
                save_config()
                print("Set as HUB + hosting service.")

            elseif cmd == "remote" then
                station_config.is_hub = false
                HUB_ID = nil
                pcall(rednet.unhost, PROTOCOLS.ping)
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

            elseif cmd == "pos" then
                if parts[2] and parts[3] and parts[4] then
                    local x, y, z = tonumber(parts[2]), tonumber(parts[3]), tonumber(parts[4])
                    if x and y and z then
                        my_x, my_y, my_z = math.floor(x), math.floor(y), math.floor(z)
                        print("Position set: " .. my_x .. ", " .. my_y .. ", " .. my_z)
                    else
                        print("Invalid coords. Usage: pos <x> <y> <z>")
                    end
                else
                    print("Current: " .. my_x .. ", " .. my_y .. ", " .. my_z)
                    print("Usage: pos <x> <y> <z>")
                end

            elseif cmd == "connect" then
                local target_id = tonumber(parts[2])
                if target_id then
                    print("Trying to reach #" .. target_id .. "...")
                    HUB_ID = target_id
                    -- Test with a ping
                    rednet.send(target_id, {
                        type = "station_ping",
                        label = station_config.label,
                        id = os.getComputerID(),
                    }, PROTOCOLS.ping)
                    local sender, msg = rednet.receive(PROTOCOLS.status, 5)
                    if sender == target_id then
                        print("Hub #" .. target_id .. " responded!")
                        if type(msg) == "table" and msg.stations then
                            route_data = msg.stations
                        end
                        register_with_hub()
                    else
                        print("No response from #" .. target_id)
                        HUB_ID = nil
                    end
                else
                    print("Usage: connect <computer_id>")
                    print("  e.g. connect 5")
                end

            elseif cmd == "unlock" then
                if switches_locked or pending_switch_lock then
                    switches_locked = false
                    switches_locked_for = nil
                    pending_switch_lock = nil
                    print("Switches unlocked (lock + pending cleared)")
                else
                    print("Switches not locked")
                end

            elseif cmd == "test" then
                -- Raw modem transmit test
                print("Testing modem transmit...")
                local m = peripheral.wrap(modem_side)
                if m then
                    print("  isOpen(65535): " .. tostring(m.isOpen(65535)))
                    print("  isOpen(" .. os.getComputerID() .. "): " .. tostring(m.isOpen(os.getComputerID())))
                    print("  isWireless: " .. tostring(m.isWireless()))
                    -- Try GPS
                    print("  GPS test (5s timeout)...")
                    local x, y, z = gps.locate(5)
                    if x then
                        print("  GPS: " .. x .. ", " .. y .. ", " .. z)
                    else
                        print("  GPS: FAILED (no response)")
                    end
                else
                    print("  Modem not found on " .. modem_side)
                end

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
                print("  connect <id> - connect to hub by ID")
                print("  unlock       - force unlock switches")
                print("  pos <x y z>  - set station position")
                print("  test         - test modem/GPS")
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
