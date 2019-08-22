require 'fluent/plugin/output'
require 'net/https'
require 'yajl'
require 'httpclient'

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
    @default_header_values = {
                                "X-Ze-Source-UUID" => "node01"
                             }
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
    @label_header_map = {
                          conf["ze_label_branch"] => "X-Ze-Source-Meta",
                          conf["ze_label_build"]  => "X-Ze-Source-Pool",
                          conf["ze_label_node"]   => "X-Ze-Source-UUID",
                          conf["ze_label_tsuite"] => "X-Ze-Window-Meta"
                        }
    @ze_tags = {
                 "ze_tag_branch" => conf["ze_tag_branch"],
                 "ze_tag_build" => conf["ze_tag_build"],
                 "ze_tag_tsuite" => conf["ze_tag_tsuite"],
                 "ze_tag_node" => conf["ze_tag_node"]
                }
    log.info("label_header_map: " + @label_header_map.to_s)
    @http                        = HTTPClient.new()
    @http.ssl_config.verify_mode = OpenSSL::SSL::VERIFY_NONE
    @http.connect_timeout        = 60
    @zapi_url = conf["ze_log_collector_url"] + "/api/v1/post"
    @auth_token = conf["ze_log_collector_token"]
    log.info("zapi_url=" + @zapi_url)
    log.info("auth_token=" + @auth_token.to_s)
    log.info("etc_hostname=" + @etc_hostname)
  end

# def format(tag, time, record)
#   record = inject_values_to_record(tag, time, record)
#   @formatter.format(tag, time, record).chomp + "\n"
# end

  def get_request_headers(record)
    headers = {}
    if record.key?("kubernetes") and not record.fetch("kubernetes").nil?
      kubernetes = record["kubernetes"]
      container_name = kubernetes["container_name"]
      host = kubernetes["host"]
      unless kubernetes["labels"].nil?
        kubernetes["labels"].each do |k, v|
          log.trace("kubernetes label: " + k)
          @label_header_map.each do |l, h|
            if k == l
              headers[h] = v
              break
            end
          end
        end
      end
      headers["X-Ze-Stream-Name"] = kubernetes["container_name"]
      if not headers.key?("X-Ze-Source-UUID")
        headers["X-Ze-Source-UUID"] = kubernetes["host"]
      end
      if not headers.key?("X-Ze-Window-Meta")
        headers["X-Ze-Window-Meta"] = kubernetes["pod_name"]
      end
    elsif record.key?("message")
      headers["X-Ze-Source-UUID"] = @etc_hostname
      if record.key?("tailed_path")
        headers["X-Ze-Stream-Name"] = File.basename(record["tailed_path"], ".*")
      end
      unless @ze_tags["ze_tag_branch"].nil? or @ze_tags["ze_tag_branch"].empty?
        headers["X-Ze-Source-Meta"] = @ze_tags["ze_tag_branch"]
      end
      unless @ze_tags["ze_tag_build"].nil? or @ze_tags["ze_tag_build"].empty?
        headers["X-Ze-Source-Pool"] = @ze_tags["ze_tag_build"]
      end
      unless @ze_tags["ze_tag_tsuite"].nil? or @ze_tags["ze_tag_tsuite"].empty?
        headers["X-Ze-Window-Meta"] = @ze_tags["ze_tag_tsuite"]
      end
      unless @ze_tags["ze_tag_node"].nil? or @ze_tags["ze_tag_node"].empty?
        headers["X-Ze-Source-UUID"] = @ze_tags["ze_tag_node"]
      end
    end

    @default_header_values.each do |k, v|
      if not headers.key?(k)
        headers[k] = v
      end
    end
    headers["X-Ze-Stream-Type"] = "native"
    # User can use node label on pod to override "host" meta data from kubernetes
    headers["Authorization"] = "Token " + @auth_token.to_s
    headers["Content-Type"] = "application/octet-stream"
    headers["Transfer-Encoding"] = "chunked"
    headers
  end

  def post_data(data, headers)
    log.trace("post_data to " + @zapi_url + ": headers: " + headers.to_s)
    myio = StringIO.new(data)
    class <<myio
      undef :size
    end
    response = @http.post(@zapi_url, myio, headers)
    unless response.ok?
      raise RuntimeError, "Failed to send data to HTTP Source. #{response.code} - #{response.body}"
    end
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
      log.trace("out_zebrium:entry: " + entry.to_s + "\n")
      record = entry[1]
      if headers.empty?
        headers = get_request_headers(record)
      end
      if record.key?("kubernetes") and not record.fetch("kubernetes").nil?
        messages.push(record["log"].chomp)
      elsif record.key?("message")
        messages.push(record["message"].chomp)
      end
    end
    post_data(messages.join("\n"), headers)
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
