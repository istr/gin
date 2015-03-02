-- gin
require 'gin.spec.init'


-- add integration runner
local IntegrationRunner = require 'gin.spec.runners.integration'

-- helpers
function hit(request)
    return IntegrationRunner.hit(request)
end

function before_hits()
    IntegrationRunner.before()
end

function after_hits()
    IntegrationRunner.after()
end
