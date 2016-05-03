#!/usr/bin/ruby

require 'open_uri_redirections'
require 'json'
require 'ostruct'



def getFreightWalmart


	sniper = "https://www2.walmart.com.br/checkout/services/simulation?postalCode=15450000&sku=2446597"

	uri = URI.parse(sniper)

	obj = JSON.parse(uri.read, object_class: OpenStruct)

	obj[0].items[0].deliveryTypes.each do |frete|
		puts frete.name
		puts frete.shippingEstimateInDays
		puts frete.price.to_f/100
	end

end


def getFreightPontoFrio

	sniper = "http://www.pontofrio.com.br/AsyncProduto.ashx"
	uri = URI(sniper)
	res = Net::HTTP.post_form(uri, 'prefixo' => '15450', 'sufixo' => '000', 'idLojista' => '10460', 'sku' => '4853364')
	obj = JSON.parse(res.body, object_class: OpenStruct)

	obj[0].ClienteEnderecoFreteEntregasTipos.each do |frete|
		puts frete.Nome
		puts frete.PrazoEntrega
		puts frete.ValorFrete
	end

end

getFreightWalmart
getFreightPontoFrio





