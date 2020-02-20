package = "kong-plugin-proxy-request-file"
version = "0.1-1"
source = {
  url = "git://github.com/dzungrevo/kong-plugins-proxy-request-file.git",
  branch = "master"
}
description = {
  summary = "This plugin allows Kong to send request multipart/form-data body file over API"
}
build = {
  type = "builtin",
  modules = {
    ["kong.plugins.proxy-request-file.handler"] = "kong/plugins/proxy-request-file/handler.lua",
    ["kong.plugins.proxy-request-file.schema"]  = "kong/plugins/proxy-request-file/schema.lua",
  }
}