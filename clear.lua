--[[

Copyright 2015 Wanderu, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

--]]

--[[ clear <ns> , [QUEUED] [WORKING] [SCHEDULED] [FAILED]

Clear a queue

Keys:
    ns: Namespace under which queue data exists.

Args:
    At least 1 of: QUEUED, WORKING, SCHEDULED, or FAILED

--]]

local fname = "cancel"
local sep = ":"
local ns = KEYS[1]
local queues = ARGS

local log_warn = function (message)
    redis.log(redis.LOG_WARNING, "<" .. fname .. ">" .. " " .. message)
end

-- validation
for q = 1, #queues do
    if queues[q] ~= "QUEUED" or queues[q] ~= "SCHEDULED" or
            queues[q] ~= "WORKING" or queues[q] ~= "FAILED" then
        return redis.error_reply("INVALID_PARAMETER: " .. queues[q])
    end
end

-- for each queue
local number_of_jobs_removed = 0
for q = 1, #queues do
    local queue = ns .. sep .. queues[q]

    -- local queue_count = redis.call("ZCOUNT", queue)

    -- for each job in queue
    local cursor = 0
    repeat
        local scan_result = redis.call("ZSCAN", queue, cursor)

        cursor = tonumber(scan_result[1])
        local job_ids = scan_result[2]

        -- for each job id, check to see if it exists
        -- every 2 b/c this is an array like: item 1, prio 1, item 2, prio 2, ...
        for j = 1, #job_ids, 2 do
            local job_id = job_ids[j]
            -- exists returns 0 if not, 1 if so
            local job_key = ns .. sep .. "JOBS" .. sep .. job_id
            -- Remove entry from queue
            -- redis.pcall("ZREM", queue, job_id)
            -- Remove job data
            redis.pcall("DEL", job_key)
        end  -- for each job id

        number_of_jobs_removed = number_of_jobs_removed + #job_ids

    until cursor == 0
    -- It's atomic so we should have iterated through all the elements

    redis.pcall("DEL", queue)
end  -- for each queue

return number_of_jobs_removed
