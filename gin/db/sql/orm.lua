-- perf
local require = require
local function tappend(t, v) t[#t+1] = v end


local SqlOrm = {}

function SqlOrm.define_model(sql_database, table_name, id_col)
    local GinModel = {}
    GinModel.__index = GinModel
    GinModel.__id_col = id_col

    -- init
    local function quote(str)
        return sql_database:quote(str)
    end
    local orm = require('gin.db.sql.' .. sql_database.options.adapter .. '.orm').new(table_name, quote)

    function GinModel.new(attrs)
        local instance = attrs or {}
        setmetatable(instance, GinModel)
        return instance
    end

    function GinModel.create(attrs)
        local sql = orm:create(attrs)
        local id_col = GinModel.__id_col or table_name .. '_id'
        local id = sql_database:execute_and_return_last_id(sql, table_name, id_col)

        local model = GinModel.new(attrs)
        model[id_col] = id

        return model
    end

    function GinModel.where(attrs, options)
        local sql = orm:where(attrs, options)
        local results = sql_database:execute(sql)

        local models = {}
        for i = 1, #results do
            tappend(models, GinModel.new(results[i]))
        end
        return models
    end

    function GinModel.all(options)
        return GinModel.where({}, options)
    end

    function GinModel.find_by(attrs, options)
        local merged_options = { limit = 1 }
        if options and options.order then
            merged_options.order = options.order
        end
        if options and options.select then
            merged_options.select = options.select
        end

        return GinModel.where(attrs, merged_options)[1]
    end

    function GinModel.delete_where(attrs, options)
        local sql = orm:delete_where(attrs, options)
        return sql_database:execute(sql)
    end

    function GinModel.delete_all(options)
        return GinModel.delete_where({}, options)
    end

    function GinModel.update_where(attrs, options)
        local sql = orm:update_where(attrs, options)
        return sql_database:execute(sql)
    end

    function GinModel:save()
        local id_col = GinModel.__id_col or table_name .. '_id'
        if self[id_col] ~= nil then
            local id = self[id_col]
            self[id_col] = nil
            local result = GinModel.update_where(self, { [id_col] = id })
            self[id_col] = id
            return result
        else
            return GinModel.create(self)
        end
    end

    function GinModel:delete()
        if self.id ~= nil then
            return GinModel.delete_where({ id = self.id })
        else
            local id_col = GinModel.__id_col or table_name .. '_id'
            if self[id_col] ~= nil then
                return GinModel.delete_where({[id_col] = self[id_col]})
            else
                error("cannot delete a model without an id")
            end
        end
    end

    return GinModel
end

return SqlOrm
