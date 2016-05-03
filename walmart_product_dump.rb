#!/usr/bin/ruby

# Grab WALMART.COM product list

# /capa-personalizada-exclusiva-sony-xperia-m5-e5603-e5606-e5633-e5643-at48/3818742/pr
# https://www2.walmart.com.br/checkout/services/simulation?postalCode=06715400&sku=2114701

require 'rubygems'
require 'nokogiri'
require 'open_uri_redirections'
require 'json'
require 'ostruct'

def dump_data(data)
	file_path = "/Users/pasilv1/Dropbox/Synced/_personal/dev"
	file = %{#{file_path}/walmart_product_datalayer.dump}
	File.open(file, 'a') {|f| f.write(%{#{data}\n})}

end

def load_product_page(product_number)
	url = %{https://www.walmart.com.br/x/#{product_number}/pr} 
end

def parse_page_via_nokogiri(number)

	dump_to_file = false

	begin
		local_page = Nokogiri::HTML(open(load_product_page(3818742),:allow_redirections => :safe))
		local_parse = local_page.css("[@class='webstore product']")

		local_parse = local_parse.xpath("//script")

		local_parse.each {|script|
			if script.to_s.match('var dataLayer')

				if script.to_s  =~ /<script>var dataLayer = \[{"product":\s\[(.*)\],/ then 
				 obj = JSON.parse($1, object_class: OpenStruct)
				 puts obj.productSku
				end
			end
		} 

	rescue OpenURI::HTTPError => error
  		response = error.io
  		response.status
  		response.string

  		puts %{[#{response.status}][#{response.string}] sleep 5s}
  		sleep(5)
	end   
end



parse_page_via_nokogiri(1)



   










