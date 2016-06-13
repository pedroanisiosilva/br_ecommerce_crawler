#!/usr/bin/ruby

site_name = ARGV[0]

valid_sites = ["americanas.com.br","pontofrio.com.br","magazineluiza.com.br","casasbahia.com.br"]

valid_sites.each do |site|
		puts %{site: #{site} | site_name: #{site_name}}

	if (site == site_name)
		puts %{-> site: #{site} | site_name: #{site_name}}
	end
end