Request = {}
Request.__index = Request

function Request.new(ngx)
    local instance = {
        ngx = ngx,
        body = ngx.req.read_body(),
        __cache = {}
    }
    setmetatable(instance, Request)
    return instance
end

function Request:__index(index)
    local out = rawget(rawget(self, '__cache'), index)
    if out then return out end

    if index == 'uri_params' then
        self.__cache[index] = ngx.req.get_uri_args()
        return self.__cache[index]

    elseif index == 'headers' then
        self.__cache[index] = ngx.req.get_headers()
        return self.__cache[index]

    elseif index == 'post_params' then
        self.__cache[index] = ngx.req.get_post_args()
        return self.__cache[index]

    else
        return rawget(self, index)
    end
end

return Request
