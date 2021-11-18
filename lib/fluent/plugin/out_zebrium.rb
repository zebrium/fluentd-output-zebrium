require 'fluent/plugin/output'
require 'net/https'
require 'yajl'
require 'httpclient'
require 'uri'
require 'json'
require 'docker'
require 'yaml'
require 'time'

$ZLOG_COLLECTOR_VERSION = '1.50.0'

class PathMappings 
  def initialize
    @active = false
    @patterns = Array.new
    @ids = Hash.new
    @cfgs = Hash.new
    @tags = Hash.new
  end

  attr_accessor :active
  attr_accessor :patterns
  attr_accessor :ids
  attr_accessor :cfgs
  attr_accessor :tags
end

class PodConfig
  def initialize
    @cfgs = Hash.new
    @atime = Time.now()
  end

  attr_accessor :cfgs
  attr_accessor :atime
end 

class PodConfigs
  def initialize
    @cfgs = Hash.new
  end
  attr_accessor :cfgs
end 

class Fluent::Plugin::Zebrium < Fluent::Plugin::Output
  Fluent::Plugin.register_output('zebrium', self)

  helpers :inject, :formatter, :compat_parameters

  DEFAULT_LINE_FORMAT_TYPE = 'stdout'
  DEFAULT_FORMAT_TYPE = 'json'
  DEFAULT_BUFFER_TYPE = "memory"
  DEFAULT_DEPLOYMENT_NAME = "default"

  config_param :ze_log_collector_url, :string, :default => ""
  config_param :ze_log_collector_token, :string, :default => ""
  config_param :ze_log_collector_type, :string, :default => "kubernetes"
  config_param :ze_host, :string, :default => ""
  config_param :ze_timezone, :string, :default => ""
  config_param :ze_deployment_name, :string, :default => ""
  config_param :ze_host_tags, :string, :default => ""
  config_param :use_buffer, :bool, :default => true
  config_param :verify_ssl, :bool, :default => false
  config_param :ze_send_json, :bool, :default => false
  config_param :ze_support_data_send_intvl, :integer, :default => 600
  config_param :log_forwarder_mode, :bool, :default => false
  config_param :ec2_api_client_timeout_secs, :integer, :default => 1
  config_param :disable_ec2_meta_data, :bool, :default => true
  config_param :ze_host_in_logpath, :integer, :default => 0 
  config_param :ze_forward_tag, :string, :default => "ze_forwarded_logs"
  config_param :ze_path_map_file, :string, :default => ""
  config_param :ze_handle_host_as_config, :bool, :default => false 

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
      log.info("add ze_tag[" + ary[0] + "]=" + ary[1])
      if ary[0] == "ze_deployment_name" and @ze_deployment_name.empty?
        log.info("Use ze_deployment_name from ze_tags")
        @ze_deployment_name = ary[1]
      else
        @ze_tags[ary[0]] = ary[1]
      end
    end
    if @ze_deployment_name.empty?
        log.info("Set deployment name to default value " + DEFAULT_DEPLOYMENT_NAME)
        @ze_deployment_name = DEFAULT_DEPLOYMENT_NAME
    end

    @path_mappings = PathMappings.new
    @pod_configs = PodConfigs.new
    read_path_mappings()
    @file_mappings = {}
    if @log_forwarder_mode
      log.info("out_zebrium running in log forwarder mode")
    else
      read_file_mappings()
      if @disable_ec2_meta_data == false
        ec2_host_meta = get_ec2_host_meta_data()
        for k in ec2_host_meta.keys do
          log.info("add ec2 meta data " + k + "=" + ec2_host_meta[k])
          @ze_tags[k] = ec2_host_meta[k]
        end
      else
        log.info("EC2 meta data collection is disabled")
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
    @zapi_token_uri.path = @zapi_token_uri.path + "/log/api/v2/token"
    @zapi_post_uri = @zapi_uri.clone
    @zapi_post_uri.path = @zapi_post_uri.path + "/log/api/v2/tmpost"
    @zapi_ingest_uri = @zapi_uri.clone
    @zapi_ingest_uri.path = @zapi_ingest_uri.path + "/log/api/v2/ingest"
    @zapi_support_uri = @zapi_uri.clone
    @zapi_support_uri.path = "/api/v2/support"
    @auth_token = conf["ze_log_collector_token"]
    log.info("ze_log_collector_vers=" + $ZLOG_COLLECTOR_VERSION)
    log.info("ze_log_collector_type=" + @ze_log_collector_type)
    log.info("ze_deployment_name=" + (conf["ze_deployment_name"].nil? ? "<not set>": conf["ze_deployment_name"]))
    log.info("log_collector_url=" + conf["ze_log_collector_url"])
    log.info("etc_hostname=" + @etc_hostname)
    log.info("ze_forward_tag=" + @ze_forward_tag)
    log.info("ze_path_map_file=" + @ze_path_map_file)
    log.info("ze_host_in_logpath=#{@ze_host_in_logpath}")
    data = {}
    data['msg'] = "log collector starting"
    send_support_data(data)
  end

# def format(tag, time, record)
#   record = inject_values_to_record(tag, time, record)
#   @formatter.format(tag, time, record).chomp + "\n"
# end

  def read_path_mappings() 
    if ze_path_map_file.length() == 0 
      return
    end
    path_map_cfg_file = @ze_path_map_file
    if not File.exist?(path_map_cfg_file)
      log.info(path_map_cfg_file + " does not exist.")
      @path_mappings.active = false
      return
    end
    @path_mappings.active = true
    pmj = ""
    log.info(path_map_cfg_file + " exists, loading path maps")
    file = File.read(path_map_cfg_file)
    begin
      pmj = JSON.parse(file)
    rescue
      log.error(path_map_cfg_file+" does not appear to contain valid JSON")
      @path_mappings.active = false
      return
    end
    log.info(pmj)
    pmj['mappings'].each { |key, value| 
      if key == 'patterns' 
        # patterns
        value.each { |pattern|
          begin
            re = Regexp.compile(pattern, Regexp::EXTENDED)
            @path_mappings.patterns.append(re)
          rescue
            log.error("Invalid path pattern '" + pattern + "' detected")
          end
        }
      elsif key == 'ids' 
        # ids
        value.each { |id| 
          @path_mappings.ids.store(id, id)
        }
      elsif key == 'configs'
        # configs
        value.each { |config|
          @path_mappings.cfgs.store(config, config)
        }
      elsif key == 'tags' 
        # tags
        value.each { |tag|
          log.info(@path_mappings.tags)
          @path_mappings.tags.store(tag, tag)
        }
      else 
          log.error("Invalid JSON key '"+key+"' detected")
      end
    }
    if @path_mappings.patterns.length() == 0 
      log.info("No patterns are defined in "+path_map_cfg_file)
      @path_mappings.active = false
    elsif @path_mappings.ids.length() == 0 and 
        @path_mappings.cfgs.length() == 0 and 
        @path_mappings.tags.length() == 0 
      log.error("No ids/configs/tag mappings are defined in "+path_map_cfg_file)
      @path_mappings.active = false
    end

  end

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

  def map_path_ids(tailed_path, ids, cfgs, tags) 
    if not @path_mappings.active 
      return
    end
    @path_mappings.patterns.each { |re| 
       res = re.match(tailed_path)
       if res
        captures = res.named_captures
        captures.each { |key, value|
          if @path_mappings.ids[key] != nil 
            ids[key] = value
          end
          if @path_mappings.cfgs[key] != nil 
            cfgs[key] = value
          end
          if @path_mappings.tags[key] != nil 
            tags[key] = value
          end
        }
      end
    }
  end

  def get_ec2_host_meta_data()
    host_meta = {}
    token = ""
    client = HTTPClient.new()
    client.connect_timeout = @ec2_api_client_timeout_secs
    begin
      log.info("Getting ec2 api token")
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
      log.info("Calling ec2 instance meta data API")
      meta_resp = client.get('http://169.254.169.254/latest/meta-data/', :header => {'X-aws-ec2-metadata-token' => token})
      log.info("Returned from c2 instance meta call")
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

  def get_host()
      host = @k8s_hostname.empty? ? @etc_hostname : @k8s_hostname
      unless @ze_tags["ze_tag_node"].nil? or @ze_tags["ze_tag_node"].empty?
        host = @ze_tags["ze_tag_node"]
      end
      return host
  end

  def get_container_meta_data(container_id)
    meta_data = {}
    begin
      container = Docker::Container.get(container_id)
      json = container.json()
      meta_data['name'] = json['Name'].sub(/^\//, '')
      meta_data['image'] = json['Config']['Image']
      meta_data['labels'] = json['Config']['Labels']
      return meta_data
    rescue
      log.info("Exception: failed to get container (#{container_id} meta data")
      return nil
    end
  end

  # save kubernetes configues, related to a specifc pod_id for 
  # potential use later for container file-based logs
  def save_kubernetes_cfgs(cfgs) 
    if (not cfgs.key?("pod_id")) or cfgs.fetch("pod_id").nil?
      return
    end
    pod_id = cfgs["pod_id"]
    if @pod_configs.cfgs.key?(pod_id)
      pod_cfg = @pod_configs.cfgs[pod_id]
    else
      pod_cfg = PodConfig.new()
    end
    pod_cfg.atime = Time.now()
    # Select which config keys to save. 
    keys = [ "cmdb_name", "namespace_name", "namespace_id", "container_name", "pod_name" ]
    for k in keys do
        if cfgs.key?(k) and not cfgs.fetch(k).nil?
          pod_cfg.cfgs[k] = cfgs[k]
        end
     end
    @pod_configs.cfgs[pod_id]=pod_cfg
  end

  # If the current configuration has a pod_id matching one of the
  # previously stored ones any associated k8s config info will be
  # added.
  def add_kubernetes_cfgs_for_pod_id(in_cfgs)
    if (not in_cfgs.key?("pod_id")) or in_cfgs.fetch("pod_id").nil?
        return in_cfgs
    end
    pod_id = in_cfgs["pod_id"]

    if not @pod_configs.cfgs.key?(pod_id)
      return in_cfgs
    end
    pod_cfgs = @pod_configs.cfgs(pod_id)

    # Ruby times are UNIX time in seconds. Toss this if unused for
    # 10 minutes as it may be outdated
    if Time.now() - pod_cfgs.atime > 60*10
      @pod_configs.cfgs.delete(pod_id)
      # while paying the cost, do a quick check for old entries
      @pod_configs.cfgs.each do |pod_id, cfg|
        if Time.now() - cfg.atime > 60*10
          @pod_configs.cfgs.delete(pod_id)
          break
        end
      end
      return in_cfgs
    end

    pod_cfgs.atime = Time.now()
    pod_cfgs.cfgs.each do |key, value|
      in_cfgs[key] = value
    end
    return in_cfgs
  end

  def get_request_headers(chunk_tag, record)
    headers = {}
    ids = {}
    cfgs = {}
    tags = {}

    # Sometimes 'record' appears to be a simple number, which causes an exception when
    # used as a hash. Until the underlying issue is addressed detect this and log.
    if record.class.name != "Hash"  or not record.respond_to?(:key?)
      log.error("Record is not a hash, unable to process (class: ${record.class.name}).")
      return false, nil, nil
    end

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
    override_deployment = ""

    record_host = ""
    if record.key?("host") and not record["host"].empty?
      record_host = record["host"]
    end
    has_container_keys = false
    if record.key?("container_id") and record.key?("container_name")
      has_container_keys = true
    end
    if chunk_tag =~ /^sysloghost\./ or chunk_tag =~ /^#{ze_forward_tag}\./
      if record_host.empty? and ze_host_in_logpath > 0 and record.key?("tailed_path")
	   tailed_path = record["tailed_path"]
	   path_components = tailed_path.split("/")
	   if path_components.length() < ze_host_in_logpath 
		log.info("Cannot find host at index #{ze_host_in_logpath} in '#{tailed_path}'")
	   else
		# note .split has empty first element from initial '/'
		record_host = path_components[ze_host_in_logpath]
	   end
      end
      log_type = "syslog"
      forwarded_log = true
      logbasename = "syslog"
      ids["app"] = logbasename
      ids["host"] = record_host
      is_container_log = false
    elsif record.key?("kubernetes") and not record.fetch("kubernetes").nil?
      kubernetes = record["kubernetes"]
      if kubernetes.key?("namespace_name") and not kubernetes.fetch("namespace_name").nil?
        namespace = kubernetes.fetch("namespace_name")
        if namespace.casecmp?("orphaned") or namespace.casecmp?(".orphaned")
          return false, nil, nil
        end
      end
      fpath = kubernetes["container_name"]
      keys = [ "namespace_name", "host", "container_name" ]
      for k in keys do
          if kubernetes.key?(k) and not kubernetes.fetch(k).nil?
            ids[k] = kubernetes[k]
            if k == "host" and @k8s_hostname.empty?
               @k8s_hostname = kubernetes[k]
            end
            # Requirement for ZS-2185 add cmdb_role, based on namespace_name
            if k == "namespace_name" 
                cfgs["cmdb_role"] = kubernetes[k].gsub("-","_")
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
      keys = [ "namespace_id", "container_name", "pod_name", "pod_id", "container_image", "container_image_id" ]
      for k in keys do
          if kubernetes.key?(k) and not kubernetes.fetch(k).nil?
            cfgs[k] = kubernetes[k]
          end
      end
      unless kubernetes["labels"].nil?
        cfgs.merge!(kubernetes["labels"])
      end
      # At this point k8s config should be set. Save these so a subsequent file-log
      # record for the same pod_id can use them.
      save_kubernetes_cfgs(cfgs)
      unless kubernetes["namespace_annotations"].nil?
        tags = kubernetes["namespace_annotations"]
        for t in tags.keys
          if t == "zebrium.com/ze_service_group" and not tags[t].empty?
            override_deployment = tags[t]
          end
        end
      end

      unless kubernetes["annotations"].nil?
        tags = kubernetes["annotations"]
        for t in tags.keys
          if t == "zebrium.com/ze_logtype" and not tags[t].empty?
            user_mapping = true
            logbasename = tags[t]
          end
          if t == "zebrium.com/ze_service_group" and not tags[t].empty?
            override_deployment = tags[t]
          end
        end
      end

      unless kubernetes["labels"].nil?
        for k in kubernetes["labels"].keys
          if k == "zebrium.com/ze_logtype" and not kubernetes["labels"][k].empty?
            user_mapping = true
            logbasename = kubernetes["labels"][k]
          end
          if k == "zebrium.com/ze_service_group" and not kubernetes["labels"][k].empty?
            override_deployment = kubernetes["labels"][k]
          end
        end
      end
      if not user_mapping
        logbasename = kubernetes["container_name"]
      end
    elsif chunk_tag =~ /^containers\./
      if record.key?("tailed_path")
        fpath = record["tailed_path"]
        fname = File.basename(fpath)
        ary = fname.split('-')
        container_id = ""
        if ary.length == 2
          container_id = ary[0]
          cm = get_container_meta_data(container_id)
          if cm.nil?
            return false, headers, nil
          end
          cfgs["container_id"] = container_id
          cfgs["container_name"] = cm['name']
          labels = cm['labels']
          for k in labels.keys do
            cfgs[k] = labels[k]
            if k == "zebrium.com/ze_logtype" and not labels[k].empty?
              user_mapping = true
              logbasename = labels[k]
            end
            if k == "zebrium.com/ze_service_group" and not labels[k].empty?
              override_deployment = labels[k]
            end
          end
          if not user_mapping
            logbasename = cm['name']
          end
          ids["app"] = logbasename
          cfgs["image"] = cm['image']
        else
          log.error("Wrong container log file: ", fpath)
        end
      else
        log.error("Missing tailed_path on logs with containers.* tag")
      end
    elsif has_container_keys
      logbasename = record['container_name'].sub(/^\//, '')
      ids["app"] = logbasename
      cfgs["container_id"] = record['container_id']
      cfgs["container_name"] = logbasename
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
      elsif chunk_tag =~ /^k8s\.events/
        logbasename = "zk8s-events"
      elsif chunk_tag =~ /^ztcp\.events\./
        ids["host"] = record_host.empty? ? "ztcp_host": record["host"]
        logbasename = record["logbasename"] ? record["logbasename"] : "ztcp_stream"
        forwarded_log = true
        log_type = "tcp_forward"
      elsif chunk_tag =~ /^zhttp\.events\./
        ids["host"] = record_host.empty? ? "ztttp_host" : record["host"]
        logbasename = record["logbasename"] ? record["logbasename"] : "zhttp_stream"
        forwarded_log = true
        log_type = "http_forward"
      else
        # Default goes to zlog-collector. Usually there are fluentd generated message
        # and our own log messages
        # for these generic messages, we will send as json messages
        return true, {}, nil
      end
      ids["app"] = logbasename
    end
    cfgs["ze_file_path"] = fpath
    if not ids.key?("host") or ids.fetch("host").nil?
      if record_host.empty?
        ids["host"] = get_host()
      else
        ids["host"] = record_host
      end
    end
    unless @ze_deployment_name.empty?
      ids["ze_deployment_name"] = @ze_deployment_name
    end
    unless override_deployment.empty?
      log.debug("Updating ze_deployment_name to '#{override_deployment}'")
      ids["ze_deployment_name"] = override_deployment
    end
    for k in @ze_tags.keys do
      tags[k] = @ze_tags[k]
    end
    tags["fluentd_tag"] = chunk_tag
    
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

    if record.key?("tailed_path")
      map_path_ids(record["tailed_path"], ids, cfgs, tags)
      add_kubernetes_cfgs_for_pod_id(cfgs)
    end

    # host should be handled as a config element instead of an id.
    # This is used when host changes frequently, causing issues with
    # detection. The actual host is stored in the cfgs metadata, and
    # a constant is stored in the ids metadata.
    # Note that a host entry must be present in ids for correct backend
    # processing, it is simply a constant at this point.
    if ze_handle_host_as_config && ids.key?("host")
      cfgs["host"] = ids["host"]
      ids["host"] = "host_in_config"
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
    headers["authtoken"] = stream_token
    headers["Content-Type"] = "application/json"
    headers["Transfer-Encoding"] = "chunked"
    return true, headers, stream_token
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
    meta_data['ze_log_collector_vers'] = $ZLOG_COLLECTOR_VERSION + "-" + @ze_log_collector_type

    headers = {}
    headers["authtoken"] = @auth_token.to_s
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
    # log.info("Event obj:" + evt_obj.to_s)

    if evt_obj.key?('lastTimestamp') and not evt_obj.fetch('lastTimestamp').nil?
      timeStamp = evt_obj["lastTimestamp"]
    elsif evt_obj.key('eventTime') and not evt_obj.fetch('eventTime').nil?
      timeStamp = evt_obj["eventTime"]
    else 
      timeStamp = ''
    end
    msg = timeStamp + " " + severity + " " + evt_str + " msg=" + evt_obj['message'].chomp
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

  def  post_message_data(send_json, headers, messages)
    @data_post_sent = @data_post_sent + 1
    if send_json
      req = {}
      req['log_type'] = 'generic'
      req['messages'] = messages
      headers = {}
      headers["authtoken"] = @auth_token
      headers["Content-Type"] = "application/json"
      resp = post_data(@zapi_ingest_uri, req.to_json, headers)
      if resp.ok? == false
        log.error("Server ingest API return error: code #{resp.code} - #{resp.body}")
      else
        @data_post_success = @data_post_success + 1
      end
    else
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
    send_json = false
    host = ''
    meta_data = {}
    last_stoken = {}
    last_headers = {}
    chunk.each do |entry|
      record = entry[1]
      if @ze_send_json == false
        if entry[1].nil?
          log.warn("nil detected, ignoring remainder of chunk")
          return
        end
        should_send, headers, cur_stoken = get_request_headers(tag, record)
        if should_send == false
          return
        end
      end

      # get_request_headers() returns empty header, it means
      # we should send json message to server
      if headers.empty? or @ze_send_json
        send_json = true
        if host.empty?
          if record.key?("host") and not record["host"].empty?
            host = record["host"]
          else
            host = get_host()
          end
          meta_data['collector'] = $ZLOG_COLLECTOR_VERSION
          meta_data['host'] = host
          meta_data['ze_deployment_name'] = @ze_deployment_name
          meta_data['tags'] = @ze_tags.dup
          meta_data['tags']['fluentd_tag'] = tag
        end
      end

      if num_records == 0 
          last_stoken = cur_stoken
          last_headers = headers
      elsif last_stoken != cur_stoken
          log.info("Streamtoken changed in chunk, num_records="+num_records.to_s)
          post_message_data(send_json, last_headers, messages)
          messages = []
          last_stoken = cur_stoken
          last_headers = headers
          num_records = 0
      end

      if entry[0].nil?
        epoch_ms = (Time.now.strftime('%s.%3N').to_f * 1000).to_i
      else
        epoch_ms = (entry[0].to_f * 1000).to_i
      end

      if send_json
        m = {}
        m['meta'] = meta_data
        m['line'] = record
        m['line']['timestamp'] = epoch_ms
        begin
          json_str = m.to_json
        rescue Encoding::UndefinedConversionError
          json_str = m.to_s.encode('UTF-8', invalid: :replace, undef: :replace, replace: '?')
        end
        messages.push(json_str)
      else
        msg_key = nil
        if not tag =~ /^k8s\.events/
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

        if tag =~ /^k8s\.events/ and record.key?('object') and record['object']['kind'] == "Event"
          line = "ze_tm=" + epoch_ms.to_s + ",msg=" + get_k8s_event_str(record)
        else
          line = "ze_tm=" + epoch_ms.to_s + ",msg=" + record[msg_key].chomp
        end
        messages.push(line)
      end
      num_records += 1
    end
    # Post remaining messages, if any
    if num_records == 0
      log.trace("Chunk has no record, no data to post")
      return
    end
    post_message_data(send_json, headers, messages)
  end

  def send_support_data(data)
    meta_data = {}
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
