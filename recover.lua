--[[ recover <ns> ,

Find any Jobs that have been abandoned.
Clean extraneous worker entries.

for each worker in <ns>:WORKERS
    if the worker is no longer active
        for each job in <ns>:WORKERS:<worker_name>
            if job key exists
                fail job appropriately (schedule or fail)
        delete key <ns>:WORKERS:<worker_name>
        remove <worker_name> from <ns>:WORKERS
--]]

local fname = "recover"
local sep = ":"

local ns = KEYS[1]

local dtutcnow = tonumber(ARGV[2])
local dtreschedule = tonumber(ARGV[3])

-- local kworking   = ns .. sep .. "WORKING"   -- Jobs that have been consumed
local kworkers   = ns .. sep .. "WORKERS"   -- Worker IDs
-- local kfailed    = ns .. sep .. "FAILED"    -- Failed Queue
-- local kscheduled = ns .. sep .. "SCHEDULED" -- Scheduled Queue
-- local kmaxjobs   = ns .. sep .. "MAXJOBS"   -- Max number of jobs allowed
-- local kmaxfailed = ns .. sep .. "MAXFAILED" -- Max number of failures allowed

local log_warn = function (message)
    redis.log(redis.LOG_WARNING, "<" .. fname .. ">" .. " " .. message)
end

local log_notice = function (message)
    redis.log(redis.LOG_NOTICE, "<" .. fname .. ">" .. " " .. message)
end

local log_verbose = function (message)
    redis.log(redis.LOG_VERBOSE, "<" .. fname .. ">" .. " " .. message)
end

local is_error = function(result)
    return type(result) == 'table' and result.err
end

local add_key = function(tab, key)
    tab[key] = true
end

local tab_as_array = function(tab)
    local keys = {}
    local n = 0
    for k, v in pairs(tab) do
        n = n+1
        keys[n] = k
    end
    return keys
end

local abandoned = {}
local kworker, kactive, worker, workers, jobid
workers = redis.call("SMEMBERS", kworkers)
for i = 1, #workers do
    worker = workers[i]
    kworker = kworkers .. sep .. worker
    kactive = kworker .. sep .. "ACTIVE"
    if redis.call("EXISTS", kactive) == 0 then
        log_notice("Found inactive worker: " .. worker .. " Reaping jobs.")
        -- gather the job ids that have been abandoned
        local jobstrs = ""
        for _, jobid in ipairs(redis.call("SMEMBERS", kworker)) do
            if jobstrs == "" then
                jobstrs = jobstrs .. jobid
            else
                jobstrs = jobstrs .. ", " .. jobid
            end
            add_key(abandoned, jobid)
        end
        log_notice("Found abandoned jobs: " .. jobstrs)
        -- remove the worker entry from the set
        redis.pcall("SREM", kworkers, worker)
        -- remove the worker set
        redis.pcall("DEL", kworker)
    end
end

abandoned = tab_as_array(abandoned) -- convert to array (int indices)
for i, jobid in ipairs(abandoned) do
    -- redis.call("EVALSHA", fail_lua_sha, 1, ns, dtutcnow, dtreschedule)

    local result
    local kjob = ns .. sep .. "JOBS" .. sep .. jobid

    -- # NOTE: Same block of code as fail.lua #

    -- ######################
    -- Remove job from WORKING queue
    -- ######################
    redis.call("ZREM", kworking, jobid)

    -- ######################
    -- Increment failure count
    -- Set failure date
    -- ######################
    local failures = tonumber(redis.pcall("HGET", kjob, "failures"))
    if failures == nil then failures = 0 end
    redis.call("HMSET", kjob,
            "failures", tostring(failures+1),
            "datefailed", tostring(dtutcnow))

    -- ######################
    -- Either add to SCHEDULED or FAILED queues depending on MAX_FAILED
    -- ######################
    local maxfailed = tonumber(redis.pcall("GET", kmaxfailed))
    if maxfailed == nil or failures >= maxfailed then
        -- ######################
        -- Move to FAILED queue, remove Job data
        -- ######################
        result = redis.pcall("ZADD", kfailed, dtutcnow, jobid);
        result = redis.pcall("DEL", kjob);
    else
        -- ######################
        -- Move to SCHEDULED queue, keep Job data
        -- ######################
        if dtreschedule == nil then
            return redis.error_reply("INVALID_DATETIME")
        end
        result = redis.pcall("ZADD", kscheduled, dtreschedule, jobid);
    end

    -- # END: fail.lua

end

return abandoned
