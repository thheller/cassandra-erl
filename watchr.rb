watch('^src/(.*)\.erl') do |m|
  mod = m[1]

  if system('rebar compile')
    system("rebar eunit suite=#{mod}")
  end
end
