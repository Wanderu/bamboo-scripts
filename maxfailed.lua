--[[ maxfailed <ns> , [<maxfailed>]

Set or Get the max number of failures before no more retries.

Errors: INVALID_PARAMETER

--]]

local fname = "maxfailed"
local sep = ":"

local log_warn = function (message)
    redis.log(redis.LOG_WARNING, "<" .. fname .. ">" .. " " .. message)
end

local ns = KEYS[1]
local maxfailed = nil

local kmaxfailed = ns .. sep .. "MAXFAILED" -- Max number of failures allowed

-- ######################
-- If the maxfailed parameter was provided, set it.
-- ######################
if table.getn(ARGV) > 0 then
    maxfailed = ARGV[1]
    -- If the maxfailed paramter was provided, check that it is a number.
    if tonumber(maxfailed) == nil then
        log_warn("INVALID_PARAMETER - " .. tostring(maxfailed) ..
                 " is not an integer.")
        return redis.error_reply("INVALID_PARAMETER")
    end
    redis.call("SET", kmaxfailed, maxfailed)
end

-- ######################
-- Return the current number
-- ######################
return redis.call("GET", kmaxfailed)
