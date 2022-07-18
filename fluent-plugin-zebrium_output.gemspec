# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |gem|
  gem.name          = "fluent-plugin-zebrium_output"
  gem.version       = `cat version.txt`
  gem.authors       = ["Zebrium, Inc"]
  gem.description   = %q{Output plugin to Zebrium HTTP LOG COLLECTOR SERVER}
  gem.summary       = %q{Zebrium fluentd output plugin}
  gem.homepage      = "https://github.com/Zebrium/fluentd-output-zebrium"
  gem.license       = "Apache-2.0"
  gem.metadata      = { "git-version" => `git log -n 1 --pretty=format:"%H"` }

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}) { |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.required_ruby_version = '>= 2.0.0'

  gem.add_development_dependency "bundler", "~> 2"
  gem.add_development_dependency 'rake', '~> 13'
  gem.add_development_dependency 'test-unit', '~> 3.1', '>= 3.1.0'
  gem.add_development_dependency 'codecov', '~> 0.1', '>= 0.1.10'
  gem.add_runtime_dependency 'fluentd', '~> 0.14', '>= 0.14.12'
  gem.add_runtime_dependency 'httpclient', '~> 2.8', '>= 2.8.0'
end