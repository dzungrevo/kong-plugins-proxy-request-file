local basic_serializer = require "kong.plugins.log-serializers.basic"
local BatchQueue = require "kong.tools.batch_queue"
local cjson = require "cjson"
local url = require "socket.url"
local socket_http = require("socket.http")
local ltn12 = require("ltn12")
local resty_http = require "resty.http"

local multipart = require "multipart"

local ngx = ngx
local body_response = ""
local cjson_encode = cjson.encode
local ngx_encode_base64 = ngx.encode_base64
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

local function copy_file(src, dest)                                                         
  local check_file = file_check(src)                                                        
  if check_file ~= nil then                                                                 
    os.execute(string.format('cp "%s" "%s"', src, dest))                                    
  else                                                                                      
    ngx.log(ngx.WARN, "check file ERRRRROR: ", check_file)                                  
  end                                                                                       
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

local function send_file(self, conf, temp_file, boundary)
  local timeout = conf.timeout
  local keepalive = conf.keepalive
  local content_type = conf.content_type
  local http_endpoint = conf.http_endpoint

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
  local respbody = {} -- for the response body    
  local result, respcode, respheaders, respstatus = http.request{
    url = http_endpoint,
    method = "POST",
    headers = {
        ["Content-Type"] =  "multipart/form-data",
		["Boundary-Parser"] =  boundary,
    },
    source = ltn12.source.file(io.open(temp_file)),
    sink = ltn12.sink.table(response_body)
  }
  
  kong.log.info("result: ", result, " - respcode: ", respcode, " - respstatus: ", respstatus)
  if not respbody then
    return nil, "failed request to " .. host .. ":" .. tostring(port) .. ": " .. err
  end
  
  local success = respstatus < 400
  local err_msg

  if not success then
    err_msg = "request to " .. host .. ":" .. tostring(port) ..
              " returned status code " .. tostring(respstatus) .. " and body " ..
              response_body
  end
  
  respbody = table.concat(respbody)
  kong.log.info("respbody: ", respbody)
  return success, err_msg
end

local function json_array_concat(entries)
  return "[" .. table_concat(entries, ",") .. "]"
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
  ngx.log(ngx.WARN, ">> temp file: " , file_name)                                         
  if not file_name then
    return nil, "failed to get request temporary file"
  end                     
  local content_type_value = req_get_headers()[CONTENT_TYPE]
  local boundary = get_boundary(content_type_value)  
  local new_temp_file = conf.temp_dest_path .. boundary:gsub("-", "")
  copy_file(file_name, new_temp_file)
  local entry_tab = {
    file_process = new_temp_file,      -- this will be available as tab.keyone or tab["keyone"]
    boundary_process = boundary, -- this uses the full syntax
  }
  local queue_id = get_queue_id(conf)
  local q = queues[queue_id]
  if not q then
    -- batch_max_size <==> conf.queue_size
    local batch_max_size = conf.queue_size or 1
    local process = function(entries)
	  local entr = entries[1]
      return send_file(self, conf, entr.file_process, entr.boundary_process)
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

  q:add(entry_tab)
end

return ProxyRequestFileHandler