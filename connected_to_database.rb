#!/usr/bin/ruby 

#execute connected to database

require 'mysql2'
require 'digest'
require 'socket'
require 'mysql'

client = Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")

results = client.query("SELECT * FROM process")


TimeFmtStr="%Y-%m-%d %H:%M:%S"


process = Hash.new

process["pid"] = Process.pid
process["execution_registry"] = Digest::SHA256.base64digest %{#{Time.new.to_i}#{process["pid"]}}
process["executed_file"] = __FILE__
process["executed_host"] = Socket.gethostname
process["self_copy"] = File.open(process["executed_file"], "rb").read
process["target_site"] = "walmart.com.br"
process["start_time"] = Time.new.strftime(TimeFmtStr)

puts process   

def insert_place(process, connection)
   statement = "INSERT INTO process (execution_registry, executed_file, executed_host, self_copy, pid,target_site,start_time)
   VALUES(\"#{process['execution_registry']}\", \"#{process['executed_file']}\", \"#{Mysql.escape_string(process['executed_host'])}\",
    \"#{Mysql.escape_string(process['self_copy'])}\", \"#{process['pid']}\",\"#{process['target_site']}\", \"#{process['start_time']}\");"				

   connection.query(statement)

   myid = connection.last_id
   puts myid
end

insert_place(process, client)


