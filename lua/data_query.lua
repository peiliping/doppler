local resty_redis = require "resty.redis"
local redis       = resty_redis:new()
redis:set_timeout(10000)
local ok , err    = redis:connect("127.0.0.1", 6379)
if err then ngx.say(err) return end

local json        = require "cjson"

local lpeg        = require 'lpeg'
local C           = lpeg.C
local Cf          = lpeg.Cf
local Cg          = lpeg.Cg
local P           = lpeg.P
local R           = lpeg.R
local S           = lpeg.S
local V           = lpeg.V

local scripts   = ngx.shared.scripts
local tableName = ngx.var.arg_table
local dateTime  = ngx.var.arg_datetime

local expression = "C1 = 1 & C1 = 2 "

local process , recycle , lastKey = {} , {} , {}
local tmpCount = 1

function selector(colNum , op , colVal)
  lastKey = {"b" , colNum .. '-' .. colVal}
  return lastKey
end

function operation(left , op , right)
  local action = ""
  if op == "&" then 
    action = "AND"
  elseif op == "|" then
    action = "OR"
  end
  local tmpKey = "__TMP-" .. tmpCount
  lastKey = {"t" , tmpKey}
  table.insert(recycle , tmpKey)
  tmpCount = tmpCount + 1
  table.insert(process , {"BITOP" , action , tmpKey , left[1] , left[2] , right[1] , right[2]})
  return {"t" , tmpKey }
end

function Blk(p)
  return p * V "Space"
end

local G = P{
    V "Space" * V "Stmt" ;
    Stmt      = Cf(V "Group" * Cg(V "LogicSig" * V "Group")^0 , operation) ,
    Group     = V "Element" + V "Open" * V "Stmt" * V "Close" ,
    Element   = Cg(Blk(V "ColNum") * Blk(V "EqSignal") * Blk(V "ColVal") / selector) ,
    LogicSig  = Blk(C(S "&|")),

    ColNum    = P "C" * C(R "09"^1) ,
    EqSignal  = C(P "=") ,
    ColVal    = C((R "az" + R "AZ" + R "09")^1) ,

    Open      = Blk(P "(") ,
    Close     = Blk(P ")") ,
    Space     = S(" \n\t")^0 ,
}

G:match(expression)
table.insert(process , {"BITCOUNT" , unpack(lastKey)}) 

local r = redis:evalsha(scripts:get("ReadScript") , 2 , tableName , dateTime , json.encode(process) , json.encode(recycle))
ngx.say(r)
redis:set_keepalive(10000, 100)