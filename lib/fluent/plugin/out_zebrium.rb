require 'fluent/plugin/output'
require 'net/https'
require 'yajl'
require 'httpclient'
require 'uri'
require 'json'
require 'docker'

$ZLOG_COLLECTOR_VERSION = '1.36.0'

class Fluent::Plugin::Zebrium < Fluent::Plugin::Output
  Fluent::Plugin.register_output('zebrium', self)

  helpers :inject, :formatter, :compat_parameters

  DEFAULT_LINE_FORMAT_TYPE = 'stdout'
  DEFAULT_FORMAT_TYPE = 'json'
  DEFAULT_BUFFER_TYPE = "memory"

  config_param :ze_log_collector_url, :string, :default => ""
  config_param :ze_log_collector_token, :string, :default => ""
  config_param :ze_host, :string, :default => ""
  config_param :ze_timezone, :string, :default => ""
  config_param :ze_deployment_name, :string, :default => ""
  config_param :ze_host_tags, :string, :default => ""
  config_param :use_buffer, :bool, :default => true
  config_param :verify_ssl, :bool, :default => false
  config_param :ze_support_data_send_intvl, :integer, :default => 600
  config_param :log_forwarder_mode, :bool, :default => false

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
    @etc_hostname = ""
    @k8s_hostname = ""
    if File.exist?("/mnt/etc/hostname")
      # Inside fluentd container
      # In that case that host /etc/hostname is a directory, we will
      # get empty string (for example, on GKE hosts). We will
      # try to get hostname from log record from kubernetes.
      if File.file?("/mnt/etc/hostname")
        File.open("/mnt/etc/hostname", "r").each do |line|
          @etc_hostname = line.strip().chomp
        end
      end
    else
      if File.exist?("/etc/hostname")
        # Run directly on host
        File.open("/etc/hostname", "r").each do |line|
          @etc_hostname = line.strip().chomp
        end
      end
      if @etc_hostname.empty?
        @etc_hostname = `hostname`.strip().chomp
      end
    end
    # Pod names can have two formats:
    # 1. <deployment_name>-84ff57c87c-pc6xm
    # 2. <deployment_name>-pc6xm
    # We use the following two regext to find deployment name. Ideally we want kubernetes filter
    # to pass us deployment name, but currently it doesn't.
    @pod_name_to_deployment_name_regexp_long_compiled = Regexp.compile('(?<deployment_name>[a-z0-9]([-a-z0-9]*))-[a-f0-9]{9,10}-[a-z0-9]{5}')
    @pod_name_to_deployment_name_regexp_short_compiled = Regexp.compile('(?<deployment_name>[a-z0-9]([-a-z0-9]*))-[a-z0-9]{5}')
    @stream_tokens = {}
    @stream_token_req_sent = 0
    @stream_token_req_success = 0
    @data_post_sent = 0
    @data_post_success = 0
    @support_post_sent = 0
    @support_post_success = 0
    @last_support_data_sent = 0
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
    @ze_tags = {}
    kvs = conf.key?('ze_host_tags') ? conf['ze_host_tags'].split(','): []
    for kv in kvs do
      ary = kv.split('=')
      if ary.length != 2 or ary[0].empty? or ary[1].empty?
        log.error("Invalid tag in ze_host_tags: #{kv}")
        continue
      end
      @ze_tags[ary[0]] = ary[1]
      log.info("add ze_tag[" + ary[0] + "]=" + ary[1])
    end

    @file_mappings = {}
    if @log_forwarder_mode
      log.info("out_zebrium running in log forwarder mode")
    else
      read_file_mappings()
      ec2_host_meta = get_ec2_host_meta_data()
      for k in ec2_host_meta.keys do
        log.info("add ec2 meta data " + k + "=" + ec2_host_meta[k])
        @ze_tags[k] = ec2_host_meta[k]
      end
    end
    @http = HTTPClient.new()
    if @verify_ssl
      @http.ssl_config.verify_mode = OpenSSL::SSL::VERIFY_PEER
      @http.ssl_config.add_trust_ca "/usr/lib/ssl/certs"
    else
      @http.ssl_config.verify_mode = OpenSSL::SSL::VERIFY_NONE
    end
    @http.connect_timeout = 60
    @zapi_uri = URI(conf["ze_log_collector_url"])
    @zapi_token_uri = @zapi_uri.clone
    @zapi_token_uri.path = "/api/v2/token"
    @zapi_post_uri = @zapi_uri.clone
    @zapi_post_uri.path = "/api/v2/tmpost"
    @zapi_support_uri = @zapi_uri.clone
    @zapi_support_uri.path = "/api/v2/support"
    @auth_token = conf["ze_log_collector_token"]
    log.info("ze_log_collector_vers=" + $ZLOG_COLLECTOR_VERSION)
    log.info("ze_deployment_name=" + (conf["ze_deployment_name"].nil? ? "<not set>": conf["ze_deployment_name"]))
    log.info("log_collector_url=" + conf["ze_log_collector_url"])
    log.info("etc_hostname=" + @etc_hostname)
    data = {}
    data['msg'] = "log collector starting"
    send_support_data(data)
  end

# def format(tag, time, record)
#   record = inject_values_to_record(tag, time, record)
#   @formatter.format(tag, time, record).chomp + "\n"
# end

  def read_file_mappings()
    file_map_cfg_file = "/etc/td-agent/log-file-map.conf"
    if not File.exist?(file_map_cfg_file)
      log.info(file_map_cfg_file + " does not exist")
      old_file_map_cfg_file = "/etc/zebrium/log-file-map.cfg"
      if not File.exist?(old_file_map_cfg_file)
        log.info(old_file_map_cfg_file + " does not exist")
        return
      end
      log.warn(old_file_map_cfg_file + " is obsolete, please move it to " + file_map_cfg_file)
      file_map_cfg_file = old_file_map_cfg_file
    end
    log.info(file_map_cfg_file + " exists")
    file = File.read(file_map_cfg_file)
    file_mappings = JSON.parse(file)

    file_mappings['mappings'].each { |item|
      if item.key?('file') and item['file'].length > 0 and item.key?('alias') and item['alias'].length > 0
        if item['file'].index(',')
          log.warn(item['file'] + " in " + file_map_cfg_file + " has comma, alias mapping must be one-to-one mapping ")
          next
        end
        if item['file'].index('*')
          log.warn(item['file'] + " in " + file_map_cfg_file + " has *, alias mapping must be one-to-one mapping ")
          next
        end
        log.info("Adding mapping " + item['file'] + " => " + item['alias'])
        @file_mappings[item['file']] = item['alias']
      end
    }
  end

  def get_ec2_host_meta_data()
    host_meta = {}
    token = ""
    client = HTTPClient.new()
    client.connect_timeout = 5
    begin
      resp = client.put('http://169.254.169.254/latest/api/token', :header => {'X-aws-ec2-metadata-token-ttl-seconds' => '21600'})
      if resp.ok?
        token = resp.body
        log.info("Got ec2 host meta token=")
      else
        log.info("Failed to get AWS EC2 host meta data API token")
      end
    rescue
       log.info("Exception: failed to get AWS EC2 host meta data API token")
       return host_meta
    end

    begin
      meta_resp = client.get('http://169.254.169.254/latest/meta-data/', :header => {'X-aws-ec2-metadata-token' => token})
      if meta_resp.ok?
        meta_data_arr = meta_resp.body.split()
        for k in ['ami-id', 'instance-id', 'instance-type', 'hostname', 'local-hostname', 'local-ipv4', 'mac', 'placement', 'public-hostname', 'public-ipv4'] do
          if meta_data_arr.include?(k)
            data_resp = client.get("http://169.254.169.254/latest/meta-data/" + k, :header => {'X-aws-ec2-metadata-token' => token})
            if data_resp.ok?
              log.info("#{k}=#{data_resp.body}")
              host_meta['ec2-' + k] = data_resp.body
            else
              log.error("Failed to get meta data with key #{k}")
            end
          end
        end
      else
       log.error("host meta data request failed: #{meta_resp}")
      end
    rescue
       log.error("host meta data post request exception")
    end
    return host_meta
  end

  def get_container_meta_data(container_id)
    meta_data = {}
    container = Docker::Container.get(container_id)
    json = container.json()
    meta_data['name'] = json['Name'].sub(/^\//, '')
    meta_data['image'] = json['Config']['Image']
    meta_data['labels'] = json['Config']['Labels']
    return meta_data
  end

  def get_request_headers(chunk_tag, record)
    headers = {}
    ids = {}
    cfgs = {}
    tags = {}

    if record.key?("docker") and not record.fetch("docker").nil?
        container_id = record["docker"]["container_id"]
        if record.key?("kubernetes") and not record.fetch("kubernetes").nil?
          cfgs["container_id"] = container_id
        else
          ids["container_id"] = container_id
        end
    end

    is_container_log = true
    log_type = ""
    forwarded_log = false
    user_mapping = false
    fpath = ""
    if chunk_tag =~ /^sysloghost\./
      log_type = "syslog"
      forwarded_log = true
      logbasename = "syslog"
      ids["app"] = logbasename
      ids["host"] = record["host"]
      is_container_log = false
    elsif record.key?("kubernetes") and not record.fetch("kubernetes").nil?
      kubernetes = record["kubernetes"]
      if kubernetes.key?("namespace_name") and not kubernetes.fetch("namespace_name").nil?
        namespace = kubernetes.fetch("namespace_name")
        if namespace.casecmp?("orphaned") or namespace.casecmp?(".orphaned")
          return false, nil
        end
      end
      logbasename = kubernetes["container_name"]
      keys = [ "namespace_name", "host", "container_name" ]
      for k in keys do
          if kubernetes.key?(k) and not kubernetes.fetch(k).nil?
            ids[k] = kubernetes[k]
            if k == "host" and @k8s_hostname.empty?
               @k8s_hostname = kubernetes[k]
            end
          end
      end

      for pattern in [ @pod_name_to_deployment_name_regexp_long_compiled, @pod_name_to_deployment_name_regexp_short_compiled ] do
          match_data = kubernetes["pod_name"].match(pattern)
          if match_data
              ids["deployment_name"] = match_data["deployment_name"]
              break
          end
      end

      keys = [ "namespace_id", "pod_name", "pod_id", "container_image", "container_image_id" ]
      for k in keys do
          if kubernetes.key?(k) and not kubernetes.fetch(k).nil?
            cfgs[k] = kubernetes[k]
          end
      end
      unless kubernetes["labels"].nil?
        cfgs.merge!(kubernetes["labels"])
      end
      unless kubernetes["annotations"].nil?
        tags = kubernetes["annotations"]
      end
      fpath = logbasename
    elsif chunk_tag =~ /^containers\./
      if record.key?("tailed_path")
        fpath = record["tailed_path"]
        fname = File.basename(fpath)
        ary = fname.split('-')
        container_id = ""
        if ary.length == 2
          container_id = ary[0]
          cm = get_container_meta_data(container_id)
          logbasename = cm['name']
          ids["app"] = logbasename
          cfgs["container_id"] = container_id
          labels = cm['labels']
          for k in labels.keys do
            cfgs[k] = labels[k]
          end
          cfgs["image"] = cm['image']
        else
          log.error("Wrong container log file: ", fpath)
        end
      else
        log.error("Missing tailed_path on logs with containers.* tag")
      end
    else
      is_container_log = false
      if record.key?("tailed_path")
        fpath = record["tailed_path"]
        fbname = File.basename(fpath, ".*")
        if @file_mappings.key?(fpath)
          logbasename = @file_mappings[fpath]
          user_mapping = true
          ids["ze_logname"] = fbname
        else
          logbasename = fbname.split('.')[0]
          if logbasename != fbname
            ids["ze_logname"] = fbname
          end
        end
      elsif record.key?("_SYSTEMD_UNIT")
        logbasename = record["_SYSTEMD_UNIT"].gsub(/\.service$/, '')
      elsif chunk_tag == "k8s.events.watch"
        logbasename = "zk8s-events"
      elsif chunk_tag =~ /^ztcp\.events\./
        ids["host"] = record["host"] ? record["host"] : "ztcp_host"
        logbasename = record["logbasename"] ? record["logbasename"] : "ztcp_stream"
        forwarded_log = true
        log_type = "tcp_forward"
      elsif chunk_tag =~ /^zhttp\.events\./
        ids["host"] = record["host"] ? record["host"] : "ztttp_host"
        logbasename = record["logbasename"] ? record["logbasename"] : "zhttp_stream"
        forwarded_log = true
        log_type = "http_forward"
      else
        # Default goes to zlog-collector. Usually there are fluentd generated message
        # and our own log messages
        logbasename = "zlog-collector"
      end
      ids["app"] = logbasename
    end
    cfgs["ze_file_path"] = fpath
    if not ids.key?("host") or ids.fetch("host").nil?
      host = @k8s_hostname.empty? ? @etc_hostname : @k8s_hostname
      unless @ze_tags["ze_tag_node"].nil? or @ze_tags["ze_tag_node"].empty?
        host = @ze_tags["ze_tag_node"]
      end
      ids["host"] = host
    end
    unless @ze_deployment_name.empty?
      ids["ze_deployment_name"] = @ze_deployment_name
    end
    for k in @ze_tags.keys do
      if k == "ze_deployment_name"
        ids["ze_deployment_name"] = @ze_tags["ze_deployment_name"]
      else
        tags[k] = @ze_tags[k]
      end
    end

    id_key = ""
    keys = ids.keys.sort
    keys.each do |k|
      if ids.key?(k)
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
        stream_token = get_stream_token(ids, cfgs, tags, logbasename, is_container_log, user_mapping,
                                        log_type, forwarded_log)
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
    return true, headers
  end

  def get_stream_token(ids, cfgs, tags, logbasename, is_container_log, user_mapping,
                       log_type, forwarded_log)
    meta_data = {}
    meta_data['stream'] = "native"
    meta_data['logbasename'] = logbasename
    meta_data['user_logbasename'] = user_mapping
    meta_data['container_log'] = is_container_log
    meta_data['log_type'] = log_type
    meta_data['forwarded_log'] = forwarded_log
    meta_data['ids'] = ids
    meta_data['cfgs'] = cfgs
    meta_data['tags'] = tags
    meta_data['tz'] = @ze_timezone.empty? ? Time.now.zone : @ze_timezone
    meta_data['ze_log_collector_vers'] = $ZLOG_COLLECTOR_VERSION

    headers = {}
    headers["Authorization"] = "Token " + @auth_token.to_s
    headers["Content-Type"] = "application/json"
    headers["Transfer-Encoding"] = "chunked"
    @stream_token_req_sent = @stream_token_req_sent + 1
    resp = post_data(@zapi_token_uri, meta_data.to_json, headers)
    if resp.ok? == false
      if resp.code == 401
        raise RuntimeError, "Invalid auth token: #{resp.code} - #{resp.body}"
      else
        raise RuntimeError, "Failed to send data to HTTP Source. #{resp.code} - #{resp.body}"
      end
    else
      @stream_token_req_success = @stream_token_req_success + 1
    end
    parse_resp = JSON.parse(resp.body)
    if parse_resp.key?("token")
      return parse_resp["token"]
    else
      raise RuntimeError, "Failed to get stream token from zapi. #{resp.code} - #{resp.body}"
    end
  end

  def post_data(uri, data, headers)
    log.trace("post_data to " + uri.to_s + ": headers: " + headers.to_s)
    myio = StringIO.new(data)
    class <<myio
      undef :size
    end
    resp = @http.post(uri, myio, headers)
    resp
  end

  def get_k8s_event_str(record)
    evt_obj = record['object']
    severity = evt_obj['type']
    if severity == "Warning"
      severity = "WARN"
    end
    if severity == "Normal"
      severity = "INFO"
    end
    evt_str = "count=" + evt_obj['count'].to_s
    if record.key?('type')
      evt_str = evt_str + " type=" + record['type']
    end
    if evt_obj.key?('source') and evt_obj['source'].key('host')
      evt_str = evt_str + " host=" + evt_obj['source']['host']
    end
    if evt_obj.key?('metadata')
      if evt_obj['metadata'].key?('name')
        evt_str = evt_str + " name=" + evt_obj['metadata']['name']
      end
      if evt_obj['metadata'].key('namespace')
        evt_str = evt_str + " namespace=" + evt_obj['metadata']['namespace']
      end
    end
    if evt_obj.key?('involvedObject')
        in_obj = evt_obj['involvedObject']
        for k in ["kind", "namespace", "name", "uid" ] do
          if in_obj.key?(k)
            evt_str = evt_str + " " + k + "=" + in_obj[k]
          end
        end
    end
    if evt_obj.key?('reason')
      evt_str = evt_str + " reason=" + evt_obj['reason']
    end
    msg = evt_obj["lastTimestamp"] + " " + severity + " " + evt_str + " msg=" + evt_obj['message'].chomp
    return msg
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

  def prepare_support_data()
    data = {}
    data['stream_token_req_sent'] = @stream_token_req_sent
    data['stream_token_req_success'] = @stream_token_req_success
    data['data_post_sent'] = @data_post_sent
    data['data_post_success'] = @data_post_success
    data['support_post_sent'] = @support_post_sent
    data['support_post_success'] = @support_post_success
    return data
  end

  def write(chunk)
    epoch = Time.now.to_i
    if epoch - @last_support_data_sent > @ze_support_data_send_intvl
      data = prepare_support_data()
      send_support_data(data)
      @last_support_data_sent = epoch
    end
    tag = chunk.metadata.tag
    messages_list = {}
    log.trace("out_zebrium: write() called tag=", tag)

    headers = {}
    messages = []
    num_records = 0
    chunk.each do |entry|
      record = entry[1]
      msg_key = nil
      if tag != "k8s.events.watch"
        # journald use key "MESSAGE" for log message
        for k in ["log", "message", "LOG", "MESSAGE" ]
          if record.key?(k) and not record.fetch(k).nil?
            msg_key = k
            break
          end
        end
        if msg_key.nil?
          next
        end
      end

      if headers.empty?
        should_send, headers = get_request_headers(tag, record)
        if should_send == false
          return
        end
      end
      if entry[0].nil?
        epoch_ms = (Time.now.strftime('%s.%3N').to_f * 1000).to_i
      else
        epoch_ms = (entry[0].to_f * 1000).to_i
      end

      if tag == "k8s.events.watch" and record.key?('object') and record['object']['kind'] == "Event"
        line = "ze_tm=" + epoch_ms.to_s + ",msg=" + get_k8s_event_str(record)
      else
        line = "ze_tm=" + epoch_ms.to_s + ",msg=" + record[msg_key].chomp
      end
      messages.push(line)
      num_records += 1
    end
    if num_records == 0
      log.trace("Chunk has no record, no data to post")
      return
    end
    @data_post_sent = @data_post_sent + 1
    resp = post_data(@zapi_post_uri, messages.join("\n") + "\n", headers)
    if resp.ok? == false
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
    else
      @data_post_success = @data_post_success + 1
    end
  end

  def send_support_data(data)
    meta_data = {}
    meta_data['auth_token'] = @auth_token.to_s
    meta_data['collector_vers'] = $ZLOG_COLLECTOR_VERSION
    meta_data['host'] = @etc_hostname
    meta_data['data'] = data

    headers = {}
    headers["Authorization"] = "Token " + @auth_token.to_s
    headers["Content-Type"] = "application/json"
    headers["Transfer-Encoding"] = "chunked"
    @support_post_sent = @support_post_sent + 1
    resp = post_data(@zapi_support_uri, meta_data.to_json, headers)
    if resp.ok? == false
      log.error("Failed to send data to HTTP Source. #{resp.code} - #{resp.body}")
    else
      @support_post_success = @support_post_success + 1
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
