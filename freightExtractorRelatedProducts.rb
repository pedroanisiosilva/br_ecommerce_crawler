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
require 'useragents'
require 'net/http'
require 'uri'
require 'logger'

JOB_MYSQL_POOL_SIZE = 1
JOB_POOL_SIZE = 10
PRODUCT_MYSQL_POOL_SIZE = 10
LOG_LEVEL = Logger::INFO

# log = Logger.new(STDOUT)
# log.debug("Created logger")
# log.info("Program started")
# log.warn("Nothing to do!")

class JobHandler

	def get_connection
		@db_job_pool.with do |db_connection|
			return db_connection
		end
	end

	def initialize(limit,site)
		@log = Logger.new(STDOUT)
		@log.level = LOG_LEVEL 
		@jobs = Queue.new
		@db_job_pool = ConnectionPool.new(size: JOB_MYSQL_POOL_SIZE, timeout: 5) { Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")}		

		statement = %{SELECT * FROM product_related WHERE target_site = '#{site}' LIMIT #{limit}}

		if (limit == -99)
			statement = %{select * from product where origin = "#{site}"}
		end

		db = self.get_connection
		@results = db.query(statement)
		@site = site

		@log.debug(%{query: #{statement}})
		@log.debug(%{results: #{@results.size}})
	end

	def run
		@results.size.times{|i| @jobs.push i}
		results = @results.each

		workers = (JOB_POOL_SIZE).times.map do
			Thread.new do
				begin      
			  		while x = @jobs.pop(true)
			  			product = PoulateFreightTable.new(results[x],@site)
			  			product.run
			  		end
				rescue ThreadError => ex
					puts "An error of type #{ex.class} happened, message is #{ex.message} [937]"
				end
			end
		end

		workers.map(&:join)
	end
end

class PoulateFreightTable

	def initialize(product,site)
		@@db_job_pool = ConnectionPool.new(size: PRODUCT_MYSQL_POOL_SIZE, timeout: 5) { Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")}		
		@db = self.get_connection
		@product = product
		@site = site
		@log = Logger.new(STDOUT)
		@log.level = LOG_LEVEL 		
	end

	def get_connection
		@@db_job_pool.with do |db_connection|
			return db_connection
		end
	end

	def parseUrl(url)
		agent = UserAgents.rand()
		page_string = ""
		begin
			open(URI.encode(url),"User-Agent" => agent) do |f|
				page_string = f.read
			end
		rescue Exception => ex
			puts "An error of type #{ex.class} happened, message is #{ex.message} [hdh]"
		end			
		page_string
	end

	def db_insertFreightData(freight_name,freight_price,freight_promise,freight_raw_data,freight_destination)

		statement = "INSERT INTO freight_data (product_id, freight_name, freight_cost, freight_promise, target_site,zip_code,freight_raw_data)
	   VALUES(\"#{@product['product_id']}\", \"#{freight_name}\", \"#{freight_price}\",\"#{freight_promise}\", 
	   \"#{@site}\",\"#{freight_destination}\",\"#{Mysql.escape_string(freight_raw_data.force_encoding(Encoding::UTF_8))}\");"		
	    
	    begin
	    	@db.query(statement)
	    rescue Exception => ex
	    	puts "An error of type #{ex.class} happened, message is #{ex.message} [7dh]"
	    end		
	end

	def getFreightMagazine(sku)
		@log.debug(%{buscando frete do sku #{sku}})

		jquery_timestamp = DateTime.now.strftime('%Q') 

		if @cep_array.nil?
			return
		end

		@cep_array.each do |cep|

			url = %{http://www.magazineluiza.com.br/produto/frete/#{cep}000/#{sku}/magazineluiza.json?_=#{jquery_timestamp}}
			url = URI.parse(url)
			req = Net::HTTP::Get.new(url.to_s)
			req.add_field("X-Requested-With", "XMLHttpRequest")
			req.add_field("user-agent", UserAgents.rand())
			req.add_field("Referer", %{http://www.magazineluiza.com.br/})

			res = Net::HTTP.start(url.host, url.port) {|http|
			  http.request(req)
			}
			@log.debug(%{url: #{url}})
			@log.debug(%{response: #{res.body}})


			begin
				obj = JSON.parse(res.body, object_class: OpenStruct)
				nome = %{none}

				self.db_insertFreightData(nome,obj.payload.shipmentValue,obj.payload.deliveryTime,res.body,%{#{cep}000})

			rescue OpenURI::HTTPError => ex
				puts "An error of type #{ex.class} happened, message is #{ex.message} [gt5]"
				sleep(3)
			end 
		end		
	end	

	def getFreightCNOVA(idLojista,sku)
		sniper = "http://www.#{@site}/AsyncProduto.ashx"
		uri = URI(sniper)

		if @cep_array.nil?
			return
		end

		@cep_array.each do |cep|
			res = Net::HTTP.post_form(uri, 'prefixo' => cep, 'sufixo' => '000', 'idLojista' => idLojista, 'sku' => sku)

			begin
				obj = JSON.parse(res.body, object_class: OpenStruct)

				if !obj[0].ClienteEnderecoFreteEntregasTipos.nil?
					obj[0].ClienteEnderecoFreteEntregasTipos.each do |frete|
						self.db_insertFreightData(frete.Nome,frete.ValorFrete,frete.PrazoEntrega,res.body,%{#{cep}000})
					end	
				end
			rescue OpenURI::HTTPError => ex
				puts "An error of type #{ex.class} happened, message is #{ex.message} [gt5]"
				sleep(3)
			end 
		end
	end

	def fetchFreightAmericanas(price,sku)
		sniper_url = ""
		@cep_array.each do |cep|
			sniper = %{http://www.americanas.com.br/api/v1/freight?cep=#{cep}000&product={"sku":"#{sku}","quantity":1,"promotionedPrice":"#{price}"}&contract=all}

			begin
				obj = JSON.parse(self.parseUrl(sniper), object_class: OpenStruct)

				obj.freightOptions.each do |frete|

				 	self.db_insertFreightData(frete.freightOptionId,frete.totalFreightPriceWithoutPromotion,frete.totalWeekdays,self.parseUrl(sniper),%{#{cep}000})
				end	
			rescue OpenURI::HTTPError => ex
				puts "An error of type #{ex.class} happened, message is #{ex.message} [736]"
				sleep(3)
			end 			    		
		end
	end	

	def fetchFreight(cep_input)
		sniper_url = ""
		@cep_array.each do |cep|
			puts %{[#{cep}]}

			begin
				if (@site == "walmart.com.br")
					sniper = %{https://www2.walmart.com.br/checkout/services/simulation?postalCode=#{cep}000&sku=#{@product['targetSkuID']}&q=#{Random.rand(99999999999)}}
				end
				obj = JSON.parse(self.parseUrl(sniper), object_class: OpenStruct)

				obj[0].items[0].deliveryTypes.each do |frete|
					frete.price = frete.price.to_f/100

				 	self.db_insertFreightData(frete.name,frete.price,frete.shippingEstimateInDays,self.parseUrl(sniper),%{#{cep}000})
				end	
			rescue OpenURI::HTTPError => ex
				puts "An error of type #{ex.class} happened, message is #{ex.message} [vcx]"
				sleep(3)
			end 			    		
		end
	end

	def parseAmericanas
		begin
			product = Hash.new
			local_page = Nokogiri::HTML(open(@product['url'],:allow_redirections => :safe))
			local_parse = local_page.xpath("//script")

			local_parse.each do |script|
				if script.to_s.match('var objProduct')
					if script.to_s  =~ /= (.*})/m then
						begin

							clean_string = $1.gsub '\'','"'
							obj = JSON.parse(clean_string, object_class: OpenStruct)

							if obj.objSKUsProduto =~ /(\d*),/
								obj.objSKUsProduto = $1
							end
							self.fetchFreightAmericanas(obj.objPrecoProd,obj.objSKUsProduto)
						rescue Exception => ex
							puts "An error of type #{ex.class} happened, message is #{ex.message} [jdu]"
						end 						
					end			
				end
			end
		rescue OpenURI::HTTPError => error
			puts "An error of type #{ex.class} happened, message is #{ex.message} [736]"
		end 
	end	

	def parseMagazine
		@log.debug(%{magazine url #{@product['url']}})

		begin
			product = Hash.new
			local_page = Nokogiri::HTML(open(@product['url'],:allow_redirections => :safe,"Accept-Encoding" => ""))
			local_parse = local_page.xpath("//script")

			local_parse.each do |script|
				if script.to_s.match('var digitalData')
					@log.debug(%{deu match})
					if script.to_s  =~ /product":(.*),\n\s+"prot/m then 
						begin
							obj = JSON.parse($1, object_class: OpenStruct)
							@log.debug(%{sku: #{obj.idSkuFull}})
							self.getFreightMagazine(obj.idSkuFull)
					    rescue Exception => ex
					    	puts "An error of type #{ex.class} happened, message is #{ex.message} [tcj]"
					    end	
					end			
				end
			end
		rescue OpenURI::HTTPError => error
			puts "An error of type #{error.class} happened, message is #{error.message} [736]"
			sleep(3)
		end 
	end			

	def parseCNOVA
		begin
			product = Hash.new
			local_page = Nokogiri::HTML(open(@product['url'],:allow_redirections => :safe))
			local_parse = local_page.xpath("//script")

			local_parse.each do |script|
				if script.to_s.match('var siteMetadata')
					if script.to_s  =~ /= (.*);/ then
						begin
							obj = JSON.parse($1, object_class: OpenStruct)
							if local_page.to_s =~ /idLojista=(\d*)/ then
								idLojista = $1
							end
							self.getFreightCNOVA(idLojista,obj.page.product.idSku)
					    rescue Exception => ex
					    	puts "An error of type #{ex.class} happened, message is #{ex.message} [tcj]"
					    end							
					end			
				end
			end
		rescue OpenURI::HTTPError => error
			puts "An error of type #{error.class} happened, message is #{error.message} [736]"
			sleep(3)
		end 
	end	

	def processPontoFrio
		self.parseCNOVA
	end

	def processCasasBahia
		self.parseCNOVA
	end

	def processAmericanas
		self.parseAmericanas
	end

	def processMagazineLuiza
		self.parseMagazine
	end

	def run
		cep_SP = ['04538','06460','01001','08210','05859']
		cep_AC = ['69914']
		cep_AL = ['57030']
		cep_AM = ['69060']
		cep_AP = ['68908']
		cep_BA = ['41701']
		cep_CE = ['60337']
		cep_DF = ['70680']
		cep_ES = ['29055']
		cep_GO = ['74110']
		cep_MA = ['65077']
		cep_MG = ['30160']
		cep_MS = ['79023']
		cep_MT = ['78050']
		cep_PA = ['66635']
		cep_PB = ['58045']
		cep_PE = ['50710']
		cep_PI = ['64057']
		cep_PR = ['80730']
		cep_RJ = ['21240']
		cep_RN = ['59086']
		cep_RO = ['76805']
		cep_RR = ['69301']
		cep_RS = ['90250']
		cep_SC = ['88034']
		cep_SE = ['49160']
		cep_TO = ['77023']
		
		@cep_array = cep_SP + cep_AC + cep_SE + cep_SC + cep_RS + cep_RR + cep_RO
		@cep_array = @cep_array + cep_RN + cep_RJ + cep_PR + cep_PI + cep_PE + cep_PB + cep_PA + cep_MT
		@cep_array = @cep_array + cep_MS + cep_MG + cep_MA + cep_GO + cep_ES + cep_DF + cep_CE + cep_BA
		@cep_array = @cep_array + cep_AP + cep_AM + cep_AL + cep_TO

		case @site
			when "casasbahia.com.br"
				self.processCasasBahia
			when "pontofrio.com.br"
				self.processPontoFrio
			when "americanas.com.br"
				self.processAmericanas
			when "magazineluiza.com.br"
				self.processMagazineLuiza				
			else
		end	
	end
end

#execution = JobHandler.new(20,-99,"walmart.com.br") # no limit on select
#execution = JobHandler.new(1000,"americanas.com.br") # limit to 10 results
#execution = JobHandler.new(5000,"walmart.com.br") # limit to 10 results, development env
execution = JobHandler.new(100,"americanas.com.br") # limit to 10 results
#execution = JobHandler.new(100,"pontofrio.com.br") # limit to 10 results, development env
#execution = JobHandler.new(100,"casasbahia.com.br") # limit to 10 results
#execution = JobHandler.new(1000,"magazineluiza.com.br") # limit to 10 results
execution.run #execute!



