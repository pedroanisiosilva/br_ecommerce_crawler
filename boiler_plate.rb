#!/usr/bin/ruby

require 'date'
require 'useragents'
require 'net/http'
require 'uri'

def revisionNumber(freight_name,zip_code,product_id,freight_cost,freight_promise)

	statement = %{SELECT f.freight_cost, f.freight_promise, f.freight_revision FROM freight_data f WHERE f.freight_name = '#{freight_name}' AND product_id = '#{product_id} AND zip_code = '#{zip_code}';}
	results = db.query(statement)

	if results.size == 0
		return 0
	end

	results.each do | result|
		if freight_promise != result["freight_promise"] || freight_cost != result["freight_cost"]
			return result["freight_revision"]+1
		end
	end
	return -1
end
