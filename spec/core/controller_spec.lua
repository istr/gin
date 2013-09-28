require 'spec/spec_helper'
local c = require 'core/controller'

describe("Controller", function()

    describe(".new", function()
        before_each(function()
            ngx = { req = { read_body = function() return "request-body" end } }
            params = {}
            controller = c.new(ngx, params)
        end)

        after_each(function()
            ngx = nil
            params = nil
            controller = nil
        end)

        it("creates a new instance of a controller", function()
            assert.are.equals(ngx, controller.ngx)
            assert.are.equals(params, controller.params)
            assert.are.equals("request-body", controller.request.body)
        end)
    end)
end)
