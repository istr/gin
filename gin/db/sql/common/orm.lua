-- perf
local next = next
local pairs = pairs
local setmetatable = setmetatable
local tconcat = table.concat
local type = type
local function tappend(t, v) t[#t+1] = v end

-- field and values helper
local function field_and_values(quote, attrs, concat)
    local fav = {}
    for field, value in pairs(attrs) do
        if true == value then
            tappend(fav, field)
        else
            local key_pair = {}
            tappend(key_pair, field)
            if type(value) ~= 'number' then
              value = quote(tostring(value))
            end
            tappend(key_pair, "=")
            tappend(key_pair, value)

            tappend(fav, tconcat(key_pair))
        end
    end
    return tconcat(fav, concat)
end

-- where
local function build_where(self, sql, attrs)
    if attrs ~= nil then
        if type(attrs) == 'table' then
            if next(attrs) ~= nil then
                tappend(sql, " WHERE (")
                tappend(sql, field_and_values(self.quote, attrs, ' AND '))
                tappend(sql, ")")
            end
        else
            tappend(sql, " WHERE (")
            tappend(sql, attrs)
            tappend(sql, ")")
        end
    end
end

-- join
local function build_join(self, sql, attrs, maincol)
    if not (attrs and attrs.table and attrs.col) then return end
    -- TODO: check that col exists in self
    local col = attrs.col
    if maincol then
        for idx, c in ipairs(maincol) do
            if c == col then
                maincol[idx] = self.table_name .. '.' .. c
            end
        end
    end
    tappend(sql, " LEFT JOIN ")
    tappend(sql, attrs.table )
    tappend(sql, " ON ")
    tappend(sql, self.table_name)
    tappend(sql, '.')
    tappend(sql, col)
    tappend(sql, '=')
    tappend(sql, attrs.table)
    tappend(sql, '.')
    tappend(sql, col)
    tappend(sql, ' ')
end

local SqlCommonOrm = {}
SqlCommonOrm.__index = SqlCommonOrm

function SqlCommonOrm.new(table_name, quote_fun)
    -- init instance
    local instance = {
        table_name = table_name,
        quote = quote_fun
    }
    setmetatable(instance, SqlCommonOrm)

    return instance
end


function SqlCommonOrm:create(attrs)
    -- health check
    if attrs == nil or next(attrs) == nil then
        error("no attributes were specified to create new model instance")
    end
    -- init sql
    local sql = {}
    -- build fields
    local fields = {}
    local values = {}
    for field, value in pairs(attrs) do
        tappend(fields, field)
        if type(value) ~= 'number' then value = self.quote(value) end
        tappend(values, value)
    end
    -- build sql
    tappend(sql, "INSERT INTO ")
    tappend(sql, self.table_name)
    tappend(sql, " (")
    tappend(sql, tconcat(fields, ','))
    tappend(sql, ") VALUES (")
    tappend(sql, tconcat(values, ','))
    tappend(sql, ");")
    -- hit server
    return tconcat(sql)
end

function SqlCommonOrm:where(attrs, options)
    -- init sql
    local sql = {}
    -- start
    tappend(sql, "SELECT ")

    if attrs and attrs.col and ('table' ~= type(attrs.col)) then error('col spec must be a table') end

    local groupby = {} -- TODO, some col/join combos may need this
    local join = {}
    -- join
    if attrs and 'string' ~= type(attrs) and attrs.join then
        for _, j in ipairs(attrs.join) do
            build_join(self, join, j, attrs.col)
        end
        attrs.join = nil
    end

    -- cols
    if attrs and attrs.col then
        if 0 < #attrs.col then
            local what = {}
            for _, col in ipairs(attrs.col) do
                tappend(what, col)
            end
            tappend(sql, tconcat(what,', '))
        end
        attrs.col = nil
    else
        tappend(sql, "*") -- TODO cols
    end

    -- main table
    tappend(sql, " FROM ")
    tappend(sql, self.table_name)

    -- join
    if 0 < #join then
        tappend(sql, tconcat(join))
    end

    -- where
    build_where(self, sql, attrs)
    -- options
    if options then
        local order = options.order
        local limit = options.limit
        local offset = options.offset
        local group = options.group

        -- group
        if group ~= nil then
            if 'table' == type(group) then
              -- TODO
            else
              tappend(groupby, self.table_name .. '.' .. group)
            end
            tappend(sql, " GROUP BY ")
            tappend(sql, tconcat(groupby, ', '))
        end
        -- order
        if order ~= nil then
            tappend(sql, " ORDER BY ")
            tappend(sql, order)
        end
        -- limit
        if limit ~= nil then
            tappend(sql, " LIMIT ")
            tappend(sql, limit)
        end
        -- offset
        if offset ~= nil then
            tappend(sql, " OFFSET ")
            tappend(sql, offset)
        end
    end
    -- close
    tappend(sql, ";")
    -- execute
    return tconcat(sql)
end

function SqlCommonOrm:delete_where(attrs, options)
    -- init sql
    local sql = {}
    -- start
    tappend(sql, "DELETE FROM ")
    tappend(sql, self.table_name)
    -- where
    build_where(self, sql, attrs)
    -- options
    if options then
        -- limit
        if options.limit ~= nil then
            tappend(sql, " LIMIT ")
            tappend(sql, options.limit)
        end
    end
    -- close
    tappend(sql, ";")
    -- execute
    return tconcat(sql)
end

function SqlCommonOrm:update_where(attrs, where_attrs)
    -- health check
    if attrs == nil or next(attrs) == nil then
        error("no attributes were specified to create new model instance")
    end
    -- init sql
    local sql = {}
    -- start
    tappend(sql, "UPDATE ")
    tappend(sql, self.table_name)
    tappend(sql, " SET ")
    -- updates
    tappend(sql, field_and_values(self.quote, attrs, ','))
    -- where
    build_where(self, sql, where_attrs)
    -- close
    tappend(sql, ";")
    -- execute
    return tconcat(sql)
end

return SqlCommonOrm
