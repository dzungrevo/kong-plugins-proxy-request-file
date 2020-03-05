local basic_serializer = require "kong.plugins.log-serializers.basic"
local BatchQueue = require "kong.tools.batch_queue"
local cjson = require "cjson"
local url = require "socket.url"
local socket_http = require("socket.http")
local ltn12 = require("ltn12")
local resty_http = require "resty.http"
-- local mp_lib = require "multipart-post"
local pcall = pcall
local multipart = require "multipart"
local uuid = require 'resty.jit-uuid'

local ngx = ngx
local body_response = ""
local ngx_encode_base64 = ngx.encode_base64
local ngx_decode_base64 = ngx.decode_base64
local table_concat = table.concat
local fmt = string.format


local ProxyRequestFileHandler = {}


ProxyRequestFileHandler.PRIORITY = 12
ProxyRequestFileHandler.VERSION = "1.0"


local queues = {} -- one queue per unique plugin config

local parsed_urls_cache = {}

local JSON, MULTI, ENCODED = "json", "multi_part", "form_encoded"
local req_get_headers = ngx.req.get_headers
local CONTENT_TYPE = "content-type"
local str_find = string.find
local get_raw_body = kong.request.get_raw_body

local b='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/' -- You will need this for encoding/decoding

-- encoding
local function encodeb64(data)
  return ((data:gsub('.', function(x) 
    local r,b='',x:byte()
    for i=8,1,-1 do r=r..(b%2^i-b%2^(i-1)>0 and '1' or '0') end
    return r;
  end)..'0000'):gsub('%d%d%d?%d?%d?%d?', function(x)
    if (#x < 6) then return '' end
    local c=0
    for i=1,6 do c=c+(x:sub(i,i)=='1' and 2^(6-i) or 0) end
    return b:sub(c+1,c+1)
  end)..({ '', '==', '=' })[#data%3+1])
end

-- decoding
local function decodeb64(data)
  data = string.gsub(data, '[^'..b..'=]', '')
  return (data:gsub('.', function(x)
    if (x == '=') then return '' end
    local r,f='',(b:find(x)-1)
    for i=6,1,-1 do r=r..(f%2^i-f%2^(i-1)>0 and '1' or '0') end
    return r;
  end):gsub('%d%d%d?%d?%d?%d?%d?%d?', function(x)
    if (#x ~= 8) then return '' end
    local c=0
    for i=1,8 do c=c+(x:sub(i,i)=='1' and 2^(8-i) or 0) end
    return string.char(c)
  end))
end

local function json_array_concat(entries)
  return "[" .. table_concat(entries, ",") .. "]"
end

local function write_file(data, file_name)
  local file_writer = io.open(file_name, "w")
  file_writer:write(cjson.encode(data))
  file_writer:close()
end

local function read_file(file_name, read_type)
  local file_handler = io.open(file_name, read_type)
  local retbyte = file_handler:read("*all")
  file_handler:close()
  return retbyte
end

local function get_content_type(content_type)                                                           
  if content_type == nil then                                                                           
    return                                                                                              
  end                                                                                                   
  if str_find(content_type:lower(), "application/json", nil, true) then                                 
    return JSON                                                                                         
  elseif str_find(content_type:lower(), "multipart/form-data", nil, true) then                          
    return MULTI                                                                                        
  elseif str_find(content_type:lower(), "application/x-www-form-urlencoded", nil, true) then            
    return ENCODED                                                                                      
  end                                                                                                   
end

local function file_check(file_name)
  local file_found=io.open(file_name, "r")            
  return file_found
end

local function copy_file(src, dest)                                                         
  local check_file = file_check(src)                                                        
  if check_file ~= nil then                                                                 
    os.execute(string.format('cp "%s" "%s"', src, dest))                                    
  else                                                                                      
    ngx.log(ngx.WARN, "check file ERRRRROR: ", check_file)                                  
  end                                                                                       
end 

local function get_boundary(content_type)
  if content_type == nil then
    return
  end
  local b_index = str_find(content_type:lower(), "boundary=", 1)
  if b_index == nil then
    return
  end
  return string.sub(content_type, b_index + string.len("boundary="))
end

local function parse_json(body)
  if body then
    local status, res = pcall(cjson.decode, body)
    if status then
      return res
    end
  end
end

local function transform_json_body(conf, body)  
  local content_length = (body and #body) or 0
  local parameters = parse_json(body)
  if parameters == nil then
    if content_length > 0 then
      return false, nil
    end
    parameters = {}
  end
  parameters.request_type = JSON
  return true, parameters
end

local function transform_multipart_body(conf, body, content_type_value)
  local content_length = (body and #body) or 0
  local boundary = get_boundary(content_type_value)
  if body == nil then
    return false, nil
  end
  local data = {
    request_file = ngx_encode_base64(body),
	boundary_parser = boundary,
	request_type = MULTI
  }
  return true, data
end

-- Parse host url.
-- @param `url` host url
-- @return `parsed_url` a table with host details:
-- scheme, host, port, path, query, userinfo
local function parse_url(host_url)
  local parsed_url = parsed_urls_cache[host_url]
  if parsed_url then
    return parsed_url
  end
  parsed_url = url.parse(host_url)
  if not parsed_url.port then
    if parsed_url.scheme == "http" then
      parsed_url.port = 80
    elseif parsed_url.scheme == "https" then
      parsed_url.port = 443
    end
  end
  if not parsed_url.path then
    parsed_url.path = "/"
  end
  parsed_urls_cache[host_url] = parsed_url
  return parsed_url
end

local function dump_table(o)
  ngx.log(ngx.WARN, "====== object type: ", type(o))
  if type(o) == 'table' then
    local count = 0
    for _ in pairs(o) do count = count + 1 end
    ngx.log(ngx.WARN, "========== TAble size: ", count)
    for k,v in pairs(o) do
      ngx.log(ngx.WARN, "============= body key: ", k)
      ngx.log(ngx.WARN, "============= body val: ", v)
    end
  else
    ngx.log(ngx.WARN, "================ table: ", tostring(o))
  end
end	

-- multipart post client              
--[[                                 
local function multipart_post(url, file_hl, file_size, entry)
  local mp = mp_lib.gen_request                                               
  local H = socket_http.request                                      
  ngx.log(ngx.WARN, "tag: ", entry.tag)                                                           
  local rq = mp{                                      
    request_file = {name = "request_body_file", data = ltn12.source.file(file_hl), len = file_size},                                                                             
  }                                                                                          
  rq.url = url   
  rq.headers.boundary_parser = entr.boundary_process
  rq.headers.service_type = entry.tag
  rq.headers.username = entry.username
  local result, respcode, respheaders, respstatus = H(rq)
  --ngx.log(ngx.WARN, "b: ", b, " == c: ", c, " == h: ", h)
  ngx.log(ngx.WARN, "result: ", result, " - respcode: ", respcode, " - respstatus: ", respstatus)
  return result, respcode, respheaders, respstatus                                               
end  
--]]

local function send_file(self, conf, payload)
  local timeout = conf.timeout
  local keepalive = conf.keepalive
  local content_type = conf.content_type
  local http_endpoint = conf.http_endpoint
  local method = conf.method
  local ok, err
  local parsed_url = parse_url(http_endpoint)
  local host = parsed_url.host
  local port = tonumber(parsed_url.port)

  local httpc = resty_http.new()
  httpc:set_timeout(timeout)
  ok, err = httpc:connect(host, port)
  if not ok then
    return nil, "failed to connect to " .. host .. ":" .. tostring(port) .. ": " .. err
  end
  
  if parsed_url.scheme == "https" then
    local _, err = httpc:ssl_handshake(true, host, false)
    if err then
      return nil, "failed to do SSL handshake with " ..
                  host .. ":" .. tostring(port) .. ": " .. err
    end
  end
  
  local res, err = httpc:request({
    method = method,
    path = parsed_url.path,
    query = parsed_url.query,
    headers = {
      ["Host"] = parsed_url.host,
      ["Content-Type"] = content_type,
      ["Content-Length"] = #payload,
      ["Authorization"] = parsed_url.userinfo and (
        "Basic " .. ngx_encode_base64(parsed_url.userinfo)
      ),
    },
    body = payload,
  })
  if not res then
    return nil, "failed request to " .. host .. ":" .. tostring(port) .. ": " .. err
  end

  -- always read response body, even if we discard it without using it on success
  local response_body = res:read_body()
  local success = res.status < 400
  local err_msg

  if not success then
    err_msg = "request to " .. host .. ":" .. tostring(port) ..
              " returned status code " .. tostring(res.status) .. " and body " ..
              response_body
  end

  ok, err = httpc:set_keepalive(keepalive)
  if not ok then
    -- the batch might already be processed at this point, so not being able to set the keepalive
    -- will not return false (the batch might not need to be reprocessed)
    kong.log.err("failed keepalive for ", host, ":", tostring(port), ": ", err)
  end

  return success, err_msg
end




local function get_queue_id(conf)
  return fmt("%s:%s:%s:%s:%s:%s",
             conf.http_endpoint,
             conf.method,
             conf.content_type,
             conf.timeout,
             conf.keepalive,
             conf.retry_count,
             conf.queue_size,
             conf.flush_timeout)
end


function ProxyRequestFileHandler:access(conf)                                 
  ngx.req.read_body()                                                         
  local file_name = ngx.req.get_body_file()      
  local body_data = "" 
  -- ngx.log(ngx.WARN, ">> temp file: " , file_name)    
  local uid = uuid.generate_v4()
  if not file_name then
    body_data = get_raw_body()
    -- return nil, "failed to get request temporary file"
  else
    -- copy_file(file_name, "/usr/local/kong/logs/" .. uid .. ".tmp")  
    body_data = read_file(file_name, "rb") 
  end      
  
  local content_type_value = req_get_headers()[CONTENT_TYPE]
  local content_type = get_content_type(content_type_value)
  local is_transform = false
  if content_type == JSON then
    is_transform, body_data = transform_json_body(conf, body_data)
  elseif content_type == MULTI then
    is_transform, body_data = transform_multipart_body(conf, body_data, content_type_value)
  end

  local ctx = kong.ctx.plugin
  kong.ctx.shared.uid = uid -- identify request id for migration purpose
  ctx.body_data = body_data
end

function ProxyRequestFileHandler:log(conf)  
  local ctx = kong.ctx.plugin  
  if not ctx.body_data then
    ngx.log(ngx.WARN, "WARNING-LOG: No temporary request file body")
    return nil, "WARNING-LOG: No temporary request file body"
  end  
  local s_tag = ""
  local s_username = ""  
  if ngx.ctx.service ~= nil then                                                                                           
    s_tag = ngx.ctx.service.tags[0] or ngx.ctx.service.tags[1]                                   
  end        
  if ngx.ctx.authenticated_consumer ~= nil then
    s_username = ngx.ctx.authenticated_consumer.username
  end  
  local entry = ctx.body_data
  entry.service_type = s_tag
  entry.username = s_username
  entry.uid = kong.ctx.shared.uid
  --write_file(entry, "/usr/local/kong/logs/body_" .. kong.ctx.shared.uid .. ".tmp") 
  
  entry = cjson.encode(entry)  
  local queue_id = get_queue_id(conf)                                                            
  local q = queues[queue_id]                                                                                               
  if not q then                                                                                                            
    -- batch_max_size <==> conf.queue_size                                                                                 
    local batch_max_size = conf.queue_size or 1                                                  
    local process = function(entries)                                                                                      
      local entr = entries[1]                                                                                          
      return send_file(self, conf, entr)           
    end                                                                       

    local opts = {
      retry_count    = conf.retry_count,
      flush_timeout  = conf.flush_timeout,
      batch_max_size = batch_max_size,
      process_delay  = 0,
    }

    local err
    q, err = BatchQueue.new(process, opts)
    if not q then
      kong.log.err("could not create queue: ", err)
      return
    end
    queues[queue_id] = q
  end

  q:add(entry)
end
return ProxyRequestFileHandler
