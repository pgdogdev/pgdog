Gem::Specification.new do |s|
  s.name        = "pgdog"
  s.version     = "0.1.1"
  s.summary     = "PgDog plugin for Ruby on Rails."
  s.description = "Add routing hints to the application to enable direct-to-shard transaction routing in ambiguous contexts."
  s.authors     = ["Lev Kokotov"]
  s.email       = "hi@pgdog.dev"
  s.files       = ["lib/pgdog.rb"]
  s.homepage    =
    "https://rubygems.org/gems/pgdog"
  s.license       = "MIT"
  s.add_dependency 'rails', '>= 5.0', '<= 9.0'
  s.add_development_dependency 'rspec', '~> 3.13'
  s.add_development_dependency 'pg', '~> 1.0'
  s.required_ruby_version = '>= 2.0'
end
