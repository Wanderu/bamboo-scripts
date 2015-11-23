--[[ maxjobs <ns> , [<maxjobs>]

Set or Get the max number of simultaneous jobs.

Errors: INVALID_PARAMETER

--]]

local fname = "maxjobs"
local sep = ":"

local log_warn = function (message)
    redis.log(redis.LOG_WARNING, "<" .. fname .. ">" .. " " .. message)
end

local ns = KEYS[1]
local maxjobs = nil

local kmaxjobs = ns .. sep .. "MAXJOBS" -- Max number of failures allowed

-- ######################
-- If the maxjobs parameter was provided, set it.
-- ######################
if table.getn(ARGV) > 0 then
    maxjobs = ARGV[1]
    -- If the maxjobs paramter was provided, check that it is a number.
    if tonumber(maxjobs) == nil then
        log_warn("INVALID_PARAMETER - " .. tostring(maxjobs) ..
                 " is not an integer.")
        return redis.error_reply("INVALID_PARAMETER")
    end
    redis.call("SET", kmaxjobs, maxjobs)
end

-- ######################
-- Return the current number
-- ######################
return redis.call("GET", kmaxjobs)
