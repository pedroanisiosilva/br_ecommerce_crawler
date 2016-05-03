#/usr/bin/ruby

require './ProcessBase.rb'
require 'rubygems'
require 'nokogiri'
require 'open_uri_redirections'
require 'json'
require 'ostruct'

class PontoFrio < ProcessBase

	def initialize
		super
		$process_id_db = register_process(__FILE__)
	end

	def select_raw_products

		jobs = Queue.new

		con = self.get_connection
		rs = con.query 'select * from raw_product_url where process_id = 24 limit 100;'

		## begin execution

		rs.size.times{|i| jobs.push i}
		results_array = rs.each.to_a

		workers = (POOL_SIZE).times.map do
		  Thread.new do
		    begin      
		      while x = jobs.pop(true)
					parse_page_via_nokogiri(%{#{results_array[x]["url"]}})
		      end
		    rescue ThreadError
		    end
		  end
		end

		workers.map(&:join)
	end

end

def parse_page_via_nokogiri(url)

	begin
		product = Hash.new
		local_page = Nokogiri::HTML(open(url,:allow_redirections => :safe))
		local_parse = local_page.xpath("//script")
		local_parse.each do |script|
			if script.to_s.match('var siteMetadata')
				if script.to_s  =~ /= (.*);/ then 

					obj = JSON.parse($1, object_class: OpenStruct)

					puts %{[#{obj.page.product.idModel}]}

				end			
			end
		end
	rescue OpenURI::HTTPError => error
  		response = error.io
  		response.status
  		response.string

  		puts %{[#{response.status}][#{response.string}] sleep 5s}
  		sleep(5)
	end 
end

p = PontoFrio.new()
p.select_raw_products



