require 'fluent/plugin/output'
require 'net/https'
require 'yajl'
require 'httpclient'
require 'json'

class Fluent::Plugin::Zebrium < Fluent::Plugin::Output
  Fluent::Plugin.register_output('zebrium', self)

  helpers :inject, :formatter, :compat_parameters

  DEFAULT_LINE_FORMAT_TYPE = 'stdout'
  DEFAULT_FORMAT_TYPE = 'json'
  DEFAULT_BUFFER_TYPE = "memory"

  config_param :ze_log_collector_url, :string, :default => ""
  config_param :ze_log_collector_token, :integer, :default => 0
  config_param :ze_label_build, :string, :default => ""
  config_param :ze_label_branch, :string, :default => ""
  config_param :ze_label_node, :string, :default => ""
  config_param :ze_label_tsuite, :string, :default => ""
  config_param :ze_tag_build, :string, :default => ""
  config_param :ze_tag_branch, :string, :default => ""
  config_param :ze_tag_tsuite, :string, :default => ""
  config_param :ze_tag_node, :string, :default => ""
  config_param :use_buffer, :bool, :default => true
  config_param :verify_ssl, :bool, :default => false

  config_section :format do
    config_set_default :@type, DEFAULT_LINE_FORMAT_TYPE
    config_set_default :output_type, DEFAULT_FORMAT_TYPE
  end

  config_section :buffer do
    config_set_default :@type, DEFAULT_BUFFER_TYPE
    config_set_default :chunk_keys, ['time']
  end

  def initialize
    super
    if File.file?("/mnt/etc/hostname")
      # Inside fluentd container
      File.open("/mnt/etc/hostname", "r").each do |line|
        @etc_hostname = line.strip().chomp
      end
    elsif File.file?("/etc/hostname")
      # Run directly on host
      File.open("/etc/hostname", "r").each do |line|
        @etc_hostname = line.strip().chomp
      end
    else
        @etc_hostname = `hostname`.strip().chomp
    end
    # Pod names can have two formats:
    # 1. <deployment_name>-84ff57c87c-pc6xm
    # 2. <deployment_name>-pc6xm
    # We use the following two regext to find deployment name. Ideally we want kubernetes filter
    # to pass us deployment name, but currently it doesn't.
    @pod_name_to_deployment_name_regexp_long_compiled = Regexp.compile('(?<deployment_name>[a-z0-9]([-a-z0-9]*))-[a-f0-9]{9,10}-[a-z0-9]{5}')
    @pod_name_to_deployment_name_regexp_short_compiled = Regexp.compile('(?<deployment_name>[a-z0-9]([-a-z0-9]*))-[a-z0-9]{5}')
    @stream_tokens = {}
  end

  def multi_workers_ready?
    false
  end

  def prefer_buffered_processing
    @use_buffer
  end

  attr_accessor :formatter

  # This method is called before starting.
  def configure(conf)
    log.info("out_zebrium::configure() called")
    compat_parameters_convert(conf, :inject, :formatter)
    super
    @formatter = formatter_create
    @ze_tags = {
                 "ze_tag_branch" => conf["ze_tag_branch"],
                 "ze_tag_build" => conf["ze_tag_build"],
                 "ze_tag_tsuite" => conf["ze_tag_tsuite"],
                 "ze_tag_node" => conf["ze_tag_node"]
                }
    log.info("label_header_map: " + @label_header_map.to_s)
    @http                        = HTTPClient.new()
    if @verify_ssl
      @http.ssl_config.verify_mode = OpenSSL::SSL::VERIFY_PEER
    else
      @http.ssl_config.verify_mode = OpenSSL::SSL::VERIFY_NONE
    end
    @http.connect_timeout        = 60
    @zapi_token_url = conf["ze_log_collector_url"] + "/api/v2/token"
    @zapi_post_url = conf["ze_log_collector_url"] + "/api/v2/tmpost"
    @auth_token = conf["ze_log_collector_token"]
    log.info("log_collector_url=" + conf["ze_log_collector_url"])
    log.info("auth_token=" + @auth_token.to_s)
    log.info("etc_hostname=" + @etc_hostname)
  end

# def format(tag, time, record)
#   record = inject_values_to_record(tag, time, record)
#   @formatter.format(tag, time, record).chomp + "\n"
# end

  def get_request_headers(record)
    headers = {}
    ids = {}
    cfgs = {}
    tags = {}

    if record.key?("docker") and not record.fetch("docker").nil?
        container_id = record["docker"]["container_id"]
        ids["container_id"] = container_id
    end

    if record.key?("kubernetes") and not record.fetch("kubernetes").nil?
      kubernetes = record["kubernetes"]
      logbasename = kubernetes["container_name"]
      keys = [
               "namespace_name", "namespace_id", "pod_name", "pod_id",
               "host", "container_name", "container_image", "container_image_id"
              ]
      for k in keys do
          if kubernetes.key?(k) and not kubernetes.fetch(k).nil?
            ids[k] = kubernetes[k]
          end
      end

      for pattern in [ @pod_name_to_deployment_name_regexp_long_compiled, @pod_name_to_deployment_name_regexp_short_compiled ] do
          match_data = kubernetes["pod_name"].match(pattern)
          if match_data
              ids["deployment_name"] = match_data["deployment_name"]
              break
          end
      end

      unless kubernetes["labels"].nil?
        cfgs = kubernetes["labels"]
      end
      unless kubernetes["annotations"].nil?
        tags = kubernetes["annotations"]
      end
    else
      host = @etc_hostname
      if record.key?("tailed_path")
        logbasename = File.basename(record["tailed_path"], ".*")
      elsif record.key?("_SYSTEMD_UNIT")
        logbasename = record["_SYSTEMD_UNIT"].gsub(/\.service$/, '')
      else
        logbasename = "na"
      end
      unless @ze_tags["ze_tag_branch"].nil? or @ze_tags["ze_tag_branch"].empty?
        cfgs["branch"] = @ze_tags["ze_tag_branch"]
      end
      unless @ze_tags["ze_tag_build"].nil? or @ze_tags["ze_tag_build"].empty?
        cfgs["build"] = @ze_tags["ze_tag_build"]
      end
      unless @ze_tags["ze_tag_node"].nil? or @ze_tags["ze_tag_node"].empty?
        host = @ze_tags["ze_tag_node"]
      end
      ids["host"] = host
      ids["app"] = logbasename
    end

    id_key = ""
    keys = ids.keys.sort
    keys.each do |k|
      if not ids[k].nil?
        if id_key.empty?
          id_key = k + "=" + ids[k]
        else
          id_key = id_key + "," + k + "=" + ids[k]
        end
      end
    end

    has_stream_token = false
    if @stream_tokens.key?(id_key)
        # Make sure there is no meta data change. If there is change, new stream token
        # must be requested.
        cfgs_tags_match = true
        if (cfgs.length == @stream_tokens[id_key]['cfgs'].length &&
                tags.length == @stream_tokens[id_key]['tags'].length)
            @stream_tokens[id_key]['cfgs'].keys.each do |k|
                old_cfg = @stream_tokens[id_key]['cfgs'][k]
                if old_cfg != cfgs[k]
                    log.info("Stream " + id_key + " config has changed: old " + old_cfg + ", new " + cfgs[k])
                    cfgs_tags_match = false
                    break
                end
            end
            @stream_tokens[id_key]['tags'].keys.each do |k|
                old_tag = @stream_tokens[id_key]['tags'][k]
                if old_tag !=  tags[k]
                    log.info("Stream " + id_key + " config has changed: old " + old_tag + ", new " + tags[k])
                    cfgs_tags_match = false
                    break
                end
            end
        else
            log.info("Stream " + id_key + " number of config or tag has changed")
            cfgs_tags_match = false
        end
        if cfgs_tags_match
            has_stream_token = true
        end
    end

    if has_stream_token
        stream_token = @stream_tokens[id_key]["token"]
    else
        log.info("Request new stream token with key " + id_key)
        stream_token = get_stream_token(ids, cfgs, tags, logbasename)
        @stream_tokens[id_key] = {
                                   "token" => stream_token,
                                   "cfgs"  => cfgs,
                                   "tags"  => tags
                                 }
    end

    # User can use node label on pod to override "host" meta data from kubernetes
    headers["Authorization"] = "Token " + stream_token
    headers["Content-Type"] = "application/json"
    headers["Transfer-Encoding"] = "chunked"
    headers
  end

  def get_stream_token(ids, cfgs, tags, logbasename)
    meta_data = {}
    meta_data['stream'] = "native"
    meta_data['logbasename'] = logbasename
    meta_data['ids'] = ids
    meta_data['cfgs'] = cfgs
    meta_data['tags'] = tags
    meta_data['tz'] = Time.now.zone

    headers = {}
    headers["Authorization"] = "Token " + @auth_token.to_s
    headers["Content-Type"] = "application/json"
    headers["Transfer-Encoding"] = "chunked"
    resp = post_data(@zapi_token_url, meta_data.to_json, headers)
    unless resp.ok?
      if resp.code == 401
        raise RuntimeError, "Invalid auth token: #{resp.code} - #{resp.body}"
      else
        raise RuntimeError, "Failed to send data to HTTP Source. #{resp.code} - #{resp.body}"
      end
    end
    parse_resp = JSON.parse(resp.body)
    if parse_resp.key?("token")
      return parse_resp["token"]
    else
      raise RuntimeError, "Failed to get stream token from zapi. #{resp.code} - #{resp.body}"
    end
  end

  def post_data(url, data, headers)
    log.trace("post_data to " + url + ": headers: " + headers.to_s)
    myio = StringIO.new(data)
    class <<myio
      undef :size
    end
    resp = @http.post(url, myio, headers)
    resp
  end

  def process(tag, es)
    es = inject_values_to_event_stream(tag, es)
    es.each {|time,record|
      if record.key?("kubernetes") and not record.fetch("kubernetes").nil?
          str = ""
          kubernetes = record["kubernetes"].clone
          container_name = kubernetes["container_name"]
          str = str + "container_name=" + container_name + ","
          host = kubernetes["host"]
          str = str + "host=" + host + ","
          kubernetes["labels"].each do |k, v|
              str = str + "label:" + k + "=" + v + ","
          end
          str = str + "\n"
      end
    }
  end

  def write(chunk)
    tag = chunk.metadata.tag
    messages_list = {}
    log.trace("out_zebrium: write() called tag=", tag)

    headers = {}
    messages = []
    chunk.each do |entry|
      record = entry[1]
      msg_key = nil
      # journald use key "MESSAGE" for log message
      for k in ["log", "message", "LOG", "MESSAGE" ]
        if record.key?(k) and not record.fetch(k).nil?
          msg_key = k
          break
        end
      end
      if msg_key.nil?
        continue
      end

      if headers.empty?
        headers = get_request_headers(record)
      end
      if entry[0].nil?
        epoch = Time.now.strftime('%s')
      else
        epoch = entry[0].to_int
      end

      line = "ze_tm=" + epoch.to_s + ",msg=" + record[msg_key].chomp
      messages.push(line)
    end
    resp = post_data(@zapi_post_url, messages.join("\n") + "\n", headers)
    unless resp.ok?
      if resp.code == 401
        # Our stream token becomes invalid for some reason, have to acquire new one.
        # Usually this only happens in testing when server gets recreated.
        # There is no harm to clear all stream tokens.
        log.error("Server says stream token is invalid: #{resp.code} - #{resp.body}")
        log.error("Delete all stream tokens")
        @stream_tokens = {}
        raise RuntimeError, "Delete stream token, and retry"
      else
        raise RuntimeError, "Failed to send data to HTTP Source. #{resp.code} - #{resp.body}"
      end
    end
  end

  # This method is called when starting.
  def start
    super
  end

  # This method is called when shutting down.
  def shutdown
    super
  end

end
