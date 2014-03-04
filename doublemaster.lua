--
-- rewrite the query according to the configure data for COM_QUERY
--
function rewrite_query( packet ) 
	local temp   = nil
	local parser = proxy.sqlparser
	proxy.queries:append(1, packet)
	if string.byte(packet) == proxy.COM_QUERY then
		if proxy.connection.intran == 0 then
			if parser:tkname(1) == "select" then
				temp = parser:comment()
				if temp ~= null and string.match(temp, "master") then
					temp = proxy.connection.get_master
				else
					temp = proxy.connection.get_read
				end
			else
				temp = proxy.connection.get_master
			end
		end
	else
		if proxy.connection.intran == 0 then
			temp = proxy.connection.get_read
		end
	end
        if proxy.connection.backend_ndx == 0 then
		proxy.response = { type = proxy.MYSQLD_PACKET_ERR, errcode  = 2013,
        	                errmsg   = "Lost connection to MySQL server during query or no managed connection avaiable",
                	        sqlstate = "HY000" } 
                return proxy.PROXY_SEND_RESULT
        end
	return proxy.PROXY_SEND_QUERY
end

