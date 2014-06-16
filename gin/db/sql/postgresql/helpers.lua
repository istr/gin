-- perf
local tconcat = table.concat


local PostgreSqlHelpers = {}

-- build location execute name
function PostgreSqlHelpers.location_for(options)
    name = {
        'gin',
        options.adapter,
        options.host,
        (options.port or ''),
        options.database,
    }
    return tconcat(name, '|'):gsub('[^%w|]', '_')
end

function PostgreSqlHelpers.execute_location_for(options)
    name = {
        PostgreSqlHelpers.location_for(options),
        'execute'
    }
    return tconcat(name, '|'):gsub('[^%w|]', '_')
end

return PostgreSqlHelpers
