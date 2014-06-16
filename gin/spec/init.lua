-- ensure test environment is specified
local posix = require 'posix'
posix.setenv('GIN_ENV', 'test')

-- gin
local original_package_path = package.path
package.path = './?.lua;' .. package.path
local Gin = require'gin.core.gin'
package.path = original_package_path .. ';' .. Gin.settings.package_path

local helpers = require 'gin.helpers.common'

-- detached
require 'gin.core.detached'

-- helpers
function pp(o)
    return helpers.pp(o)
end
