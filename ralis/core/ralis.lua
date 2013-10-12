require 'ralis.core.error'
require 'ralis.core.helpers'
require 'ralis.core.request'
require 'ralis.core.response'
require 'ralis.core.routes'

-- libraries
JSON = require 'cjson'
local lfs = require"lfs"

-- init ralis if necessary
Ralis = Ralis or {}

-- version
Ralis.version = '0.1'

-- environment
Ralis.env = Ralis.env or os.getenv("RALIS_ENV") or 'development'

-- settings
local settings = require 'ralis.core.settings'
Ralis.settings = settings.for_current_environment()

-- set ralis database
Ralis.database = {
    adapter = 'mysql',
    host = "127.0.0.1",
    port = 3306,
    database = "ralis_development",
    user = "root",
    password = "",
    pool = 5
}

-- load initializers
local function run_initializers(path)
    if folder_exists(path) then
        for file_name in lfs.dir(path) do
            if file_name ~= "." and file_name ~= ".." then
                local file_path = path .. '/' .. file_name
                local attr = lfs.attributes(file_path)
                assert(type(attr) == "table")
                if attr.mode == "directory" then
                    -- recursive call for all subdirectories inside of initializers
                    run_initializers(file_path)
                else
                    -- run initializer
                    dofile(file_path)
                end
            end
        end
    end
end
run_initializers("config/initializers")

return Ralis
