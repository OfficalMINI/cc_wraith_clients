-- =============================================
-- WRAITH OS - TRAIN CLIENT
-- =============================================
-- Run on wireless modem PCs onboard trains/minecarts.
-- GPS tracks position, maps track dynamically,
-- calculates speed/heading, and syncs with Wraith OS.
--
-- Setup: Place computer in/on minecart with wireless modem.
--        Optional: attach monitor for onboard display.
--        Run: train_client

local CLIENT_TYPE = "train_client"

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
    ping      = "wraith_rail_tr_ping",
    status    = "wraith_rail_tr_status",
    register  = "wraith_rail_tr_register",
    command   = "wraith_rail_tr_cmd",
    heartbeat = "wraith_rail_tr_hb",
    trackdata = "wraith_rail_tr_track",
}
local UPDATE_URL = "https://raw.githubusercontent.com/OfficalMINI/cc_wraith_clients/refs/heads/main/train_client.lua"
local HEARTBEAT_INTERVAL = 5
local DISCOVERY_INTERVAL = 10
local DISCOVERY_TIMEOUT = 3
local GPS_POLL_INTERVAL = 0.5
local TRACK_SAMPLE_DISTANCE = 3
local TRACK_BATCH_SIZE = 20     -- send batch every N new points
local TRACK_BATCH_INTERVAL = 10 -- or every N seconds

-- ========================================
-- Config Persistence
-- ========================================
local CONFIG_FILE = "train_config.lua"

local train_config = {
    label = nil,
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
                        train_config[k] = v
                    end
                end
            end
        end
    end
end

local function save_config()
    local f = fs.open(CONFIG_FILE, "w")
    if f then
        f.write(textutils.serialise(train_config))
        f.close()
    end
end

load_config()

if not train_config.label then
    train_config.label = "Train " .. os.getComputerID()
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
-- Monitor (optional onboard display)
-- ========================================
local monitor = peripheral.find("monitor")
if monitor then
    monitor.setTextScale(0.5)
    print("Onboard monitor found")
end

-- ========================================
-- GPS & Track State
-- ========================================
local cur_x, cur_y, cur_z = 0, 0, 0
local prev_x, prev_y, prev_z = 0, 0, 0
local speed = 0
local heading = "?"
local gps_ok = false

-- Track mapping buffer
local track_buffer = {}
local last_track_send = os.clock()

-- Route data received from Wraith
local route_info = nil

local function distance_3d(x1, y1, z1, x2, y2, z2)
    local dx = x1 - x2
    local dy = y1 - y2
    local dz = z1 - z2
    return math.sqrt(dx * dx + dy * dy + dz * dz)
end

local function compute_heading(dx, dz)
    if math.abs(dx) > math.abs(dz) then
        return dx > 0 and "East" or "West"
    elseif math.abs(dz) > 0.1 then
        return dz > 0 and "South" or "North"
    end
    return "?"
end

-- ========================================
-- Update Check (GitHub HTTP)
-- ========================================
local function check_for_updates()
    print("[update] Checking github...")
    local ok, resp = pcall(http.get, UPDATE_URL)
    if not ok or not resp then
        print("[update] Fetch failed")
        return false
    end
    local content = resp.readAll()
    resp.close()
    if not content or #content < 100 then
        print("[update] Bad response (" .. (content and #content or 0) .. "b)")
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
    local x, y, z = gps.locate(2)
    if x then
        cur_x, cur_y, cur_z = math.floor(x), math.floor(y), math.floor(z)
        gps_ok = true
    end

    rednet.broadcast({
        type = "train",
        label = train_config.label,
        id = os.getComputerID(),
        x = cur_x, y = cur_y, z = cur_z,
    }, PROTOCOLS.ping)

    local sender, msg = rednet.receive(PROTOCOLS.status, DISCOVERY_TIMEOUT)
    if sender and type(msg) == "table" and msg.status == "wraith_rail_hub" then
        WRAITH_ID = sender
        if msg.stations then
            route_info = msg.stations
        end
        print("Found Wraith OS at #" .. sender)
        return true
    end
    return false
end

local function register_with_wraith()
    if not WRAITH_ID then return false end
    rednet.send(WRAITH_ID, {
        label = train_config.label,
        x = cur_x, y = cur_y, z = cur_z,
    }, PROTOCOLS.register)
    local sender, msg = rednet.receive(PROTOCOLS.status, 3)
    if sender == WRAITH_ID and type(msg) == "table" and msg.status == "registered" then
        print("Registered with Wraith #" .. WRAITH_ID)
        return true
    end
    return false
end

-- Discovery loop
while not WRAITH_ID do
    if not discover_wraith() then
        print("Wraith not found, retrying in " .. DISCOVERY_INTERVAL .. "s...")
        os.sleep(DISCOVERY_INTERVAL)
    end
end

local reg = false
for attempt = 1, 5 do
    if register_with_wraith() then
        reg = true
        break
    end
    os.sleep(2)
end
if not reg then
    print("WARNING: Could not confirm registration")
end

-- ========================================
-- Display
-- ========================================
term.clear()
term.setCursorPos(1, 1)
print("=== Train Client v" .. VERSION .. " ===")
print("Computer #" .. os.getComputerID())
print("Train:     " .. train_config.label)
print("Wraith:    #" .. tostring(WRAITH_ID))
print("Modem:     " .. modem_side)
print("Monitor:   " .. (monitor and "YES" or "NO"))
print("")
print("GPS tracking active...")
print("")

-- ========================================
-- Onboard Monitor Render
-- ========================================
local function render_onboard()
    if not monitor then return end

    local mw, mh = monitor.getSize()
    monitor.setBackgroundColor(colors.black)
    monitor.clear()

    -- Title
    monitor.setCursorPos(1, 1)
    monitor.setTextColor(colors.cyan)
    monitor.write(train_config.label)

    -- GPS status
    monitor.setCursorPos(1, 2)
    if gps_ok then
        monitor.setTextColor(colors.lime)
        monitor.write(string.format("GPS: %d, %d, %d", cur_x, cur_y, cur_z))
    else
        monitor.setTextColor(colors.red)
        monitor.write("GPS: NO FIX")
    end

    -- Speed & heading
    monitor.setCursorPos(1, 3)
    monitor.setTextColor(colors.white)
    monitor.write(string.format("Speed: %.1f  Dir: %s", speed, heading))

    -- Connection
    monitor.setCursorPos(1, 4)
    if WRAITH_ID then
        monitor.setTextColor(colors.lime)
        monitor.write("Wraith: Connected")
    else
        monitor.setTextColor(colors.red)
        monitor.write("Wraith: Disconnected")
    end

    -- Separator
    monitor.setCursorPos(1, 5)
    monitor.setTextColor(colors.gray)
    monitor.write(string.rep("-", mw))

    -- Track buffer info
    monitor.setCursorPos(1, 6)
    monitor.setTextColor(colors.gray)
    monitor.write(string.format("Track buf: %d pts", #track_buffer))

    -- Nearby stations (from route_info)
    if route_info and route_info.stations then
        monitor.setCursorPos(1, 8)
        monitor.setTextColor(colors.cyan)
        monitor.write("STATIONS:")

        local sy = 9
        local stations_sorted = {}
        for id, st in pairs(route_info.stations) do
            local dist = distance_3d(cur_x, cur_y, cur_z, st.x or 0, st.y or 0, st.z or 0)
            table.insert(stations_sorted, {st = st, dist = dist})
        end
        table.sort(stations_sorted, function(a, b) return a.dist < b.dist end)

        for _, entry in ipairs(stations_sorted) do
            if sy > mh then break end
            monitor.setCursorPos(2, sy)
            local st = entry.st
            local prefix = st.is_hub and "\4 " or "  "
            monitor.setTextColor(colors.white)
            monitor.write(prefix .. (st.label or "?"):sub(1, mw - 10))
            monitor.setTextColor(colors.gray)
            monitor.write(string.format(" %dm", math.floor(entry.dist)))
            sy = sy + 1
        end
    end
end

-- ========================================
-- Main Loops
-- ========================================

local function gps_loop()
    while true do
        local x, y, z = gps.locate(2)
        if x then
            prev_x, prev_y, prev_z = cur_x, cur_y, cur_z
            cur_x, cur_y, cur_z = math.floor(x), math.floor(y), math.floor(z)
            gps_ok = true

            -- Calculate speed (blocks per second)
            local dist = distance_3d(cur_x, cur_y, cur_z, prev_x, prev_y, prev_z)
            speed = dist / GPS_POLL_INTERVAL

            -- Calculate heading
            local dx = cur_x - prev_x
            local dz = cur_z - prev_z
            if dist > 0.5 then
                heading = compute_heading(dx, dz)
            end

            -- Track mapping: add point if moved enough
            if dist >= TRACK_SAMPLE_DISTANCE then
                table.insert(track_buffer, {x = cur_x, y = cur_y, z = cur_z})
            end
        else
            gps_ok = false
            speed = 0
        end

        os.sleep(GPS_POLL_INTERVAL)
    end
end

local function track_sender()
    while true do
        os.sleep(1)
        local now = os.clock()
        local should_send = #track_buffer >= TRACK_BATCH_SIZE
            or (now - last_track_send >= TRACK_BATCH_INTERVAL and #track_buffer > 0)

        if should_send and WRAITH_ID then
            -- Send batch
            local batch = {}
            for i = 1, math.min(#track_buffer, TRACK_BATCH_SIZE) do
                table.insert(batch, table.remove(track_buffer, 1))
            end
            rednet.send(WRAITH_ID, {
                points = batch,
                train_id = os.getComputerID(),
            }, PROTOCOLS.trackdata)
            last_track_send = now
        end
    end
end

local function heartbeat_sender()
    while true do
        sleep(HEARTBEAT_INTERVAL)
        if WRAITH_ID then
            rednet.send(WRAITH_ID, {
                id = os.getComputerID(),
                label = train_config.label,
                x = cur_x, y = cur_y, z = cur_z,
                speed = speed,
                heading = heading,
            }, PROTOCOLS.heartbeat)
        end
    end
end

local function command_listener()
    while true do
        local sender, msg, proto = rednet.receive(nil, 1)

        if sender and proto == PROTOCOLS.command and type(msg) == "table" then
            if msg.action == "set_label" then
                train_config.label = msg.label
                save_config()
                print("Label set to: " .. train_config.label)

            elseif msg.action == "update_routes" then
                if msg.stations then
                    route_info = msg
                end
            end

        elseif sender and proto == PROTOCOLS.status then
            if type(msg) == "table" and msg.stations then
                route_info = msg.stations
            end
        end
    end
end

local function discovery_loop()
    local missed_pings = 0
    while true do
        sleep(60)
        if WRAITH_ID then
            rednet.send(WRAITH_ID, {
                type = "train",
                label = train_config.label,
                id = os.getComputerID(),
                x = cur_x, y = cur_y, z = cur_z,
            }, PROTOCOLS.ping)
            local _, resp = rednet.receive(PROTOCOLS.status, 5)
            if not resp then
                missed_pings = missed_pings + 1
                if missed_pings >= 3 then
                    print("Lost connection. Rediscovering...")
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
                    route_info = resp.stations
                end
            end
        end
    end
end

local function display_loop()
    while true do
        render_onboard()
        os.sleep(1)
    end
end

local function update_checker()
    while true do
        sleep(300)
        check_for_updates()
    end
end

parallel.waitForAll(
    gps_loop,
    track_sender,
    heartbeat_sender,
    command_listener,
    discovery_loop,
    display_loop,
    update_checker
)
