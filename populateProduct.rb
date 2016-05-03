#/usr/bin/ruby

require './ProcessBase.rb'
require 'rubygems'
require 'nokogiri'
require 'open_uri_redirections'
require 'json'
require 'ostruct'

class PoulateProduct < ProcessBase

	def initialize
		super
		$process_id_db = register_process(__FILE__)
	end

	def select_raw_products

		jobs = Queue.new

		con = self.get_connection
		rs = con.query 'select * from raw_product_url where process_id = 22 limit 400;'

		## begin execution

		rs.size.times{|i| jobs.push i}
		results_array = rs.each.to_a

		workers = (POOL_SIZE).times.map do
		  Thread.new do
		    begin      
		      while x = jobs.pop(true)
					parse_page_via_nokogiri(%{https://www.walmart.com.br#{results_array[x]["url"]}})
		      end
		    rescue ThreadError
		    end
		  end
		end

		workers.map(&:join)

	end

	def insert_product(product)
		con = self.get_connection

	   statement = "INSERT INTO product (name, brandName, departmentName, categoryName, subcategoryName,model)
	   VALUES(\"#{product['name']}\", \"#{product['brandName']}\", \"#{product['departmentName']}\",
	    \"#{product['categoryName']}\", \"#{product['subcategoryName']}\",\"#{product['model']}\");"

	    begin
	    	con.query(statement)
	    rescue
	    end

	end

end

def parse_page_via_nokogiri(url)

	dump_to_file = false

	product = Hash.new

	can_save = true;

	begin
		local_page = Nokogiri::HTML(open(url,:allow_redirections => :safe))
		local_parse = local_page.css("[@class='webstore product']")
		local_parse = local_parse.xpath("//script")
		local_parse.each {|script|

			if script.to_s.match('var dataLayer')

				if script.to_s  =~ /<script>var dataLayer = \[{"product":\s\[(.*)\],/ then 

					begin
						obj = JSON.parse($1, object_class: OpenStruct)
					rescue
						can_save = false
					end

					if(can_save)
					 	product["name"] = obj.productName
					 	product["brandName"] = obj.productBrandName
					 	product["departmentName"] = obj.productDepartmentName
					 	product["categoryName"] = obj.productCategoryName
					 	product["subcategoryName"] = obj.productSubCategory
					 end
				end
			end
		} 

		local_parse = local_page.css("[@class='value-field Referencia-do-Modelo']")


		if local_parse.to_s =~ />(.*)</ && can_save then
				product["model"] = $1.downcase!				 

		end

	rescue OpenURI::HTTPError => error
  		response = error.io
  		response.status
  		response.string

  		puts %{[#{response.status}][#{response.string}] sleep 5s}
  		sleep(5)
	end 
	if (can_save)
		insert_product(product)
	end
end

p = PoulateProduct.new()

p.select_raw_products



