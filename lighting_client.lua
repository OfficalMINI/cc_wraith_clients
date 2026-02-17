-- =============================================
-- LIGHTING CLIENT - Rainbow Lamp Controller
-- =============================================
-- Run on each lamp PC. Registers with Wraith OS
-- and executes color/pattern commands.
--
-- Setup: Place computer on top of Rainbow Lamp.
--        Attach wireless modem (any side).
--        Run: lighting_client

local CLIENT_TYPE = "lighting_client"

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

local LAMP_SIDES = {"bottom", "back"}
local WRAITH_ID = nil
local PROTOCOLS = {
    ping      = "wraith_light_ping",
    status    = "wraith_light_status",
    register  = "wraith_light_register",
    command   = "wraith_light_cmd",
    heartbeat = "wraith_light_hb",
}
local UPDATE_URL = "https://raw.githubusercontent.com/OfficalMINI/cc_wraith_clients/refs/heads/main/lighting_client.lua"
local HEARTBEAT_INTERVAL = 5
local DISCOVERY_INTERVAL = 10
local DISCOVERY_TIMEOUT = 3

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
    print("ERROR: No modem found!")
    print("Attach a wireless modem and restart.")
    return
end
rednet.open(modem_side)
print("Modem opened on: " .. modem_side)

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
-- Position Detection
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
        print("Controllers need GPS for proximity matching!")
    end
end

-- ========================================
-- Pattern Engine
-- ========================================
local current_colors = {0}
local current_pattern = "solid"
local pattern_running = true

local function set_lamp(strength)
    local val = math.floor(math.max(0, math.min(15, strength)))
    for _, side in ipairs(LAMP_SIDES) do
        redstone.setAnalogOutput(side, val)
    end
end

local function run_solid()
    while pattern_running and current_pattern == "solid" do
        set_lamp(current_colors[1] or 0)
        os.sleep(0.5)
    end
end

local function run_pulse()
    while pattern_running and current_pattern == "pulse" do
        local color = current_colors[1] or 0
        -- Ramp up
        for i = 0, color do
            if not pattern_running or current_pattern ~= "pulse" then return end
            set_lamp(i)
            os.sleep(0.1)
        end
        os.sleep(0.3)
        -- Ramp down
        for i = color, 0, -1 do
            if not pattern_running or current_pattern ~= "pulse" then return end
            set_lamp(i)
            os.sleep(0.1)
        end
        os.sleep(0.3)
    end
end

local function run_strobe()
    local idx = 1
    while pattern_running and current_pattern == "strobe" do
        local color = current_colors[idx] or current_colors[1] or 0
        set_lamp(color)
        os.sleep(0.15)
        set_lamp(0)
        os.sleep(0.15)
        idx = (idx % #current_colors) + 1
    end
end

local function run_fade()
    if #current_colors < 2 then
        run_pulse()
        return
    end
    local idx = 1
    while pattern_running and current_pattern == "fade" do
        local from = current_colors[idx]
        local next_idx = (idx % #current_colors) + 1
        local to = current_colors[next_idx]
        local steps = math.abs(to - from)
        if steps == 0 then steps = 1 end
        local dir = (to > from) and 1 or -1
        for s = 0, steps do
            if not pattern_running or current_pattern ~= "fade" then return end
            set_lamp(from + s * dir)
            os.sleep(0.15)
        end
        os.sleep(0.2)
        idx = next_idx
    end
end

local pattern_fns = {
    solid = run_solid,
    pulse = run_pulse,
    strobe = run_strobe,
    fade = run_fade,
}

-- ========================================
-- Discovery & Registration
-- ========================================
local function discover_wraith()
    print("Searching for Wraith OS...")
    rednet.broadcast("ping", PROTOCOLS.ping)
    local sender, msg, proto = rednet.receive(PROTOCOLS.status, DISCOVERY_TIMEOUT)
    if sender and type(msg) == "table" and msg.status == "wraith" then
        WRAITH_ID = sender
        print("Found Wraith OS at #" .. sender)
        return true
    end
    return false
end

local function register_with_wraith()
    if not WRAITH_ID then return false end
    rednet.send(WRAITH_ID, {
        x = my_x,
        y = my_y,
        z = my_z,
        side = table.concat(LAMP_SIDES, ","),
    }, PROTOCOLS.register)
    local sender, msg = rednet.receive(PROTOCOLS.status, 3)
    if sender == WRAITH_ID and type(msg) == "table" and msg.status == "registered" then
        print("Registered with Wraith #" .. WRAITH_ID)
        return true
    end
    print("Registration not acknowledged, retrying...")
    return false
end

-- ========================================
-- Main
-- ========================================

-- Discovery loop
while not WRAITH_ID do
    if not discover_wraith() then
        print("Wraith not found, retrying in " .. DISCOVERY_INTERVAL .. "s...")
        os.sleep(DISCOVERY_INTERVAL)
    end
end

-- Registration loop
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
    print("Continuing anyway - Wraith may still send commands")
end

-- Display status
term.clear()
term.setCursorPos(1, 1)
print("=== Lighting Client v" .. VERSION .. " ===")
print("Computer #" .. os.getComputerID())
print("Lamp sides: " .. table.concat(LAMP_SIDES, ", "))
print("Wraith:    #" .. tostring(WRAITH_ID))
print("Position:  " .. my_x .. ", " .. my_y .. ", " .. my_z)
print("Modem:     " .. modem_side)
print("")
print("Listening for commands...")
print("")

-- Network + pattern loops in parallel
local function network_loop()
    local next_heartbeat = os.clock() + HEARTBEAT_INTERVAL
    local next_rediscovery = os.clock() + 60

    while true do
        local sender, msg, proto = rednet.receive(nil, 1)

        if sender and proto == PROTOCOLS.command then
            if type(msg) == "table" and msg.action == "set" then
                local new_colors = msg.colors or {0}
                local new_pattern = msg.pattern or "solid"
                -- Only interrupt if something changed
                local changed = (new_pattern ~= current_pattern)
                if not changed then
                    if #new_colors ~= #current_colors then
                        changed = true
                    else
                        for i, c in ipairs(new_colors) do
                            if c ~= current_colors[i] then changed = true; break end
                        end
                    end
                end
                if changed then
                    current_colors = new_colors
                    current_pattern = new_pattern
                    pattern_running = false
                    os.sleep(0.05)
                    pattern_running = true
                    -- Log to terminal
                    local color_str = ""
                    for _, c in ipairs(current_colors) do
                        color_str = color_str .. tostring(c) .. " "
                    end
                    print(string.format("[%s] colors=[%s] pattern=%s",
                        os.date("%H:%M:%S") or "?", color_str, current_pattern))
                end
            end
        elseif sender and proto == PROTOCOLS.status then
            if type(msg) == "table" and msg.status == "unknown" then
                print("Re-registering with Wraith...")
                register_with_wraith()
            end
        end

        local now = os.clock()

        -- Heartbeat
        if now >= next_heartbeat then
            if WRAITH_ID then
                rednet.send(WRAITH_ID, {
                    id = os.getComputerID(),
                    time = now,
                }, PROTOCOLS.heartbeat)
            end
            next_heartbeat = now + HEARTBEAT_INTERVAL
        end

        -- Periodic re-discovery check
        if now >= next_rediscovery then
            if WRAITH_ID then
                rednet.send(WRAITH_ID, "ping", PROTOCOLS.ping)
            end
            next_rediscovery = now + 60
        end
    end
end

local function pattern_loop()
    while true do
        pattern_running = true
        local fn = pattern_fns[current_pattern] or pattern_fns.solid
        fn()
        os.sleep(0.05)
    end
end

local function update_checker()
    while true do
        sleep(300)
        check_for_updates()
    end
end

-- Turn off lamp on startup
set_lamp(0)

parallel.waitForAll(network_loop, pattern_loop, update_checker)
