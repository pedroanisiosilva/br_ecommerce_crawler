#/usr/bin/ruby

require './ProcessBase.rb'
require 'rubygems'
require 'nokogiri'
require 'open_uri_redirections'
require 'json'
require 'ostruct'
require 'connection_pool'
require 'mysql2'
require 'mysql'

class PontoFrio < ProcessBase

	def initialize
		super
		$process_id_db = register_process(__FILE__)
	end

	def select_raw_products

		jobs = Queue.new
		con = self.get_connection
		statement = 'select * from raw_product_url where process_id = 24 limit 100;'

	    begin
	    	rs = con.query(statement)
	    rescue Exception => ex
			puts "An error of type #{ex.class} happened, message is #{ex.message} [0]"
			return
	    end

		rs.size.times{|i| jobs.push i}
		results_array = rs.each.to_a

		workers = (POOL_SIZE).times.map do
		  Thread.new do
		    begin      
		      while x = jobs.pop(true)
		      		db = self.get_connection
					parse_page_via_nokogiri(%{#{results_array[x]["url"]}},db)
		      end
		    rescue ThreadError
		    end
		  end
		end

		workers.map(&:join)
	end

end

def check_if_has_walmart_product(model,db)

		statement = %{select * from product where model LIKE "%#{model}%";} 

	    begin
	    	rs = db.query(statement)
	    rescue Exception => ex
			puts "An error of type #{ex.class} happened, message is #{ex.message} [1]"
			sleep(3)
			return false
	    end	

		if (rs.count > 0)
			return true
		else
			return false
		end
end

def getFreightPontoFrio(idProduct,idLojista,sku,db)

	target_site = "pontofrio.com.br"
	cep_array = ["15450","04510"]
	sniper = "http://www.pontofrio.com.br/AsyncProduto.ashx"
	uri = URI(sniper)	

	cep_array.each do |cep|

		res = Net::HTTP.post_form(uri, 'prefixo' => cep, 'sufixo' => '000', 'idLojista' => idLojista, 'sku' => sku)
		obj = JSON.parse(res.body, object_class: OpenStruct)

		obj[0].ClienteEnderecoFreteEntregasTipos.each do |frete|
			puts frete.Nome
			puts frete.PrazoEntrega
			puts frete.ValorFrete

			statement = "INSERT INTO freight_data (product_id, freight_name, freight_cost, freight_promise, target_site,zip_code)
		   VALUES(\"#{idProduct}\", \"#{frete.Nome}\", \"#{frete.ValorFrete}\",
		    \"#{frete.PrazoEntrega}\", \"#{target_site}\",\"#{cep}000\");"
		    begin
		    	rs = db.query(statement)
		    rescue Exception => ex
		    end	
		end		
	end
end

def parse_page_via_nokogiri(url,db)

	begin
		product = Hash.new
		local_page = Nokogiri::HTML(open(url,:allow_redirections => :safe))
		local_parse = local_page.xpath("//script")
		local_parse.each do |script|
			if script.to_s.match('var siteMetadata')
				if script.to_s  =~ /= (.*);/ then 
					obj = JSON.parse($1, object_class: OpenStruct)
					if check_if_has_walmart_product(obj.page.product.idModel,db)

						if local_page.to_s =~ /idLojista=(\d*)/ then

							idLojista = $1

							statement = %{select id from product where model LIKE "%#{obj.page.product.idModel}%";} 

	    					begin
	    						rs = db.query(statement)
	    					rescue Exception => ex
								puts "An error of type #{ex.class} happened, message is #{ex.message} [4]"
								sleep(3)
	    					end	

							rs.each do |row|

							  puts row["id"] 
							  getFreightPontoFrio(row["id"],idLojista,obj.page.product.idSku,db)

							end
						end

					else
						#puts %{nao tem no Walmart [#{obj.page.product.idModel}]}
					end
				end			
			end
		end
	rescue OpenURI::HTTPError => error
  		response = error.io
  		response.status
  		response.string

  		#puts %{[#{response.status}][#{response.string}] sleep 5s}
  		sleep(5)
	end 
end

p = PontoFrio.new()
p.select_raw_products