-- perf
local error = error
local pairs = pairs
local setmetatable = setmetatable


local Controller = {}
Controller.__index = Controller

function Controller.new(request, params)
    local instance = {
        params = params or {},
        request = request
    }
    setmetatable(instance, Controller)
    return instance
end

function Controller:raise_error(code, custom_attrs)
    error({ code = code, custom_attrs = custom_attrs })
end

function Controller:accepted_params(param_filters, params)
    local accepted_params = {}
    if params then
        for _, param in pairs(param_filters) do
            accepted_params[param] = params[param]
        end
    end
    return accepted_params
end

return Controller
