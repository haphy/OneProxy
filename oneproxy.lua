--
-- Data Server Groups
-- Every Data Server Has Master-Master Structure with Semisync Enabled for Failsafe
--
local server_groups=
	{
		default={
				{addr="192.168.1.139:3306", backend=-1}
			},
                data1={
				{addr="192.168.1.139:3306", backend=-1}
                      }
	}

--
-- Fined Server Group by Table Name When No Partition Key Value Specified
-- Used For Management Works
--
local table_server_mapping={
		my_test1_0="data1",
		my_test1_1="data1",
		my_test1_2="data1",
		my_test1_3="data1",
		my_test2_0="data1",
		my_test2_1="data1",
		my_test2_2="data1",
		my_test2_3="data1",
		my_test2_4="data1",
                my_test3_0="data1",
                my_test3_1="data1",
                my_test3_2="data1",
                my_test3_3="data1",
                my_test3_4="data1",
	}
--
-- Fined Server Group by Table and  Partition Key Value
-- Four Partition Methods Supported: range, list, random, hash
-- Notes: partitions should be ordered accored to values for range partition
--
local table_partition_mapping={
                my_test1={
			col="id", 
			coltype="int",
			mode="range",
                        partitions=
			{
                                { name="my_test1_0", server="data1", minval=nil,    maxval=100000},
                                { name="my_test1_1", server="data1", minval=100000, maxval=200000},
                                { name="my_test1_2", server="data1", minval=200000, maxval=300000},
                                { name="my_test1_3", server="data1", minval=300000, maxval=nil}
                        }
                },
                my_test2={
                        col="id",
                        coltype="int",
                        mode="list",
                        partitions=
                        {
                                { name="my_test2_0", server="data1", values={1,5,9}  },
                                { name="my_test2_1", server="data1", values={2,6,10} },
                                { name="my_test2_2", server="data1", values={3,7,11} },
                                { name="my_test2_3", server="data1", values={4,8,12} },
				{ name="my_test2_4", server="data1", values=nil	     }
                        }
                },
		my_test3={
			mode="random",
                        partitions=
                        {
                                { name="my_test3_0", server="data1" },
                                { name="my_test3_1", server="data1" },
                                { name="my_test3_2", server="data1" },
                                { name="my_test3_3", server="data1" },
                                { name="my_test3_4", server="data1" }
                        }
		}
        }

--
-- Get the backend index accord to the address (ip:port) information
--
function get_backend_index(address)
	local backends = proxy.global.backends
	for ndx = 1 , #backends do
		if backends[ndx].dst.name == address then
			return ndx
		end
	end
	return 0
end

--
-- Set Proxy Backend Index by Data Group Name
-- 
function choose_server_group(grpname)
	local srvgrp = server_groups[grpname]
	local choosed_backend = 0
	if srvgrp then
		for i = 1 , #srvgrp do
			if srvgrp[i].backend == -1 then
				srvgrp[i].backend = get_backend_index(srvgrp[i].addr)
			end
			if srvgrp[i].backend > 0 then
				proxy.connection.backend_ndx = srvgrp[i].backend
				if proxy.connection.backend_ndx > 0 then
					choosed_backend = srvgrp[i].backend
					return choosed_backend
				end
			end
		end
	end
	return choosed_backend
end

--
-- Set Proxy Backend Index by Table Name 
--
function choose_server_by_table(tabname)
	if table_server_mapping[tabname] then
		return choose_server_group(table_server_mapping[tabname])
	end
	return 0
end

--
-- Get Matched Range Partition Index
--
function get_range_partition(pkey_value, partitions)
        for pndx = 1, #partitions do
		if  (partitions[pndx].minval == nil or pkey_value >= partitions[pndx].minval) and
                    (partitions[pndx].maxval == nil or pkey_value < partitions[pndx].maxval) then
                        	return pndx
		end
	end
	return 0
end

--
-- Get Matched List Partition Index
--
function get_list_partition(pkey_value, partitions)
	for pndx = 1, #partitions do
		local partitions_values = partitions[pndx].values
               	if partitions_values == nil then
			return pndx
		else
			for lvndx = 1, #partitions_values do
				if partitions_values[lvndx] == pkey_value then
					return pndx
				end
			end
		end
	end
        return 0
end


--
-- Set Proxy Backend Index by Table Name and Partition Key Values
-- If not partitioned tables, set the proxy backend index by table name
--
function choose_server_by_parser()
	local choosed_backend = 0
	local choosed_count   = 0
	local parser = proxy.sqlparser
	local part_key_index  = 0
	local tablist=parser:tables()
	local is_partition_table = 0
	for tndx = 1 , #tablist do
		local part = table_partition_mapping[tablist[tndx]]	
		if part then
			is_partition_table = 1
			break
		end
	end
	if is_partition_table == 1 then
	    for tndx = 1 , #tablist do
		local part = table_partition_mapping[tablist[tndx]]
		if part then
			local partitions = part.partitions
			if part["mode"] == "range" then
				local part_key_values = parser:values(tndx, part["col"])
				for vndx = 1, #part_key_values do
					local pkey_value = nil
					if part["coltype"] == "int" then
						pkey_value = tonumber(part_key_values[vndx])
					else
						pkey_value = part_key_values[vndx]
					end
					if part_key_index == 0 then
						part_key_index = get_range_partition(pkey_value, partitions)
						choosed_count = choosed_count + 1
					else
						local tmp_key_index = get_range_partition(pkey_value, partitions)
						if tmp_key_index > 0 and part_key_index ~= tmp_key_index then
							choosed_count = choosed_count + 1
						end
					end
				end
                                if choosed_count == 1 and part_key_index > 0 then
                                        if partitions[part_key_index].name then
                                                parser:rename(tndx, partitions[part_key_index].name)
                                        end
                                        if choosed_backend == 0 then
                                                if partitions[part_key_index].server then
                                                        choosed_backend = choose_server_group(partitions[part_key_index].server)
                                                else
                                                        choosed_backend = choose_server_by_table(partitions[part_key_index].name)
                                                end
                                        end
                                end
			elseif part["mode"] == "list" then
				local part_key_values = parser:values(tndx, part["col"])
                                for vndx = 1, #part_key_values do
                                        if part["coltype"] == "int" then
                                                pkey_value = tonumber(part_key_values[vndx])
                                        else
                                                pkey_value = part_key_values[vndx]
                                        end
                                        if part_key_index == 0 then
                                                part_key_index = get_list_partition(pkey_value, partitions)
						choosed_count = choosed_count + 1
                                        else
                                                local tmp_key_index = get_list_partition(pkey_value, partitions)
                                                if tmp_key_index > 0 and part_key_index ~= tmp_key_index then
                                                        choosed_count = choosed_count + 1
                                                end
                                        end
				end
                                if choosed_count == 1 and part_key_index > 0 then
					if partitions[part_key_index].name then
                                         	parser:rename(tndx, partitions[part_key_index].name)
					end
                                        if choosed_backend == 0 then
                                                if partitions[part_key_index].server then
                                                        choosed_backend = choose_server_group(partitions[part_key_index].server)
                                                else
                                                        choosed_backend = choose_server_by_table(partitions[part_key_index].name)
                                                end
                                        end
				end
			elseif part["mode"] == "random" then
				if part_key_index == 0 then
					math.randomseed(os.time())
					part_key_index = math.random(#partitions)
					choosed_count  = choosed_count + 1
				end
                                if partitions[part_key_index].name then
                                        parser:rename(tndx, partitions[part_key_index].name)
                                end
                                if partitions[part_key_index].server then
                                        choosed_backend = choose_server_group(partitions[part_key_index].server)
                                else
                                        choosed_backend = choose_server_by_table(partitions[part_key_index].name)
                                end
			end
		end
	    end
	else
 	    local choosed_daga_group = nil
 	    for tndx = 1 , #tablist do
		if table_server_mapping[tablist[tndx]] then
			if choosed_daga_group == nil then
				choosed_daga_group = table_server_mapping[tablist[tndx]]
				choosed_count  = choosed_count + 1
			else
				if choosed_daga_group ~= table_server_mapping[tablist[tndx]] then
					choosed_count  = choosed_count + 1
				end
			end
		end
	    end
	    if  choosed_daga_group then
		if choosed_count == 1 then
			choosed_backend = choose_server_by_table(tablist[tndx]) 
		end
  	    end
	end
	return choosed_backend, choosed_count, is_partition_table
end

--
-- rewrite the query according to the configure data for COM_QUERY
--
function rewrite_query( packet ) 
	local choosed_backend = 0
	local choosed_count   = 0
	local is_partition_table = 0
	if string.byte(packet) == proxy.COM_QUERY then
		local parser = proxy.sqlparser
		choosed_backend, choosed_count, is_partition_table = choose_server_by_parser()
		if is_partition_table == 1 then
			if choosed_count == 1 then
				if choosed_backend > 0 then
					proxy.queries:append(1, string.char(proxy.COM_QUERY) .. parser:rewrite())
				end
			elseif choosed_count > 0 then
				proxy.connection.backend_ndx = 0
				proxy.response = {
						type = proxy.MYSQLD_PACKET_ERR,	
						errcode  = 1146,
						errmsg   = "Quuery involed multiple partitions, not supported!",
						sqlstate = "42S02"
					}
				return proxy.PROXY_SEND_RESULT
			else
				if choosed_backend == 0 then
					choosed_backend = choose_server_group("default")
				end
				proxy.queries:append(1, packet)
			end
		else
			if choosed_backend == 0 then
				choosed_backend = choose_server_group("default")
			end
			proxy.queries:append(1, packet)
		end
	else
		proxy.queries:append(1, packet)
		choosed_backend = choose_server_group("default")
	end
        if proxy.connection.backend_ndx == 0 then
		proxy.response = {
                        	type = proxy.MYSQLD_PACKET_ERR,
	                        errcode  = 2013,
        	                errmsg   = "Lost connection to MySQL server during query or no managed connection avaiable",
                	        sqlstate = "HY000"
                      	} 
                return proxy.PROXY_SEND_RESULT
        end
	return proxy.PROXY_SEND_QUERY
end



