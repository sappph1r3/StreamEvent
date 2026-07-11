--[[
	StreamEvent.lua
	A wrapper around RBXScriptConnections to provide a stream-like interactions for events.
]]

--!strict
--!native

local ReplicatedStorage = game:GetService("ReplicatedStorage")

local StreamConnection = {}
StreamConnection.__index = StreamConnection

export type Connection = {
	_signal: any,
	_fn: (...any) -> (),
	Connected: boolean,
	Disconnect: (self: Connection) -> (),
}

function StreamConnection.new(signal: any, fn: (...any) -> ()): Connection
	local obj = setmetatable({
		_signal = signal,
		_fn = fn,
		Connected = true,
	}, StreamConnection)

	StreamConnection.__call = StreamConnection.Disconnect  :: any

	return obj :: Connection
end

function StreamConnection:Disconnect()
	if not self.Connected then return end
	self.Connected = false
	
	local signal = self._signal
	if not signal or not signal._connections then return end

	-- Instant removal instead of table scanning loops
	local connections = signal._connections :: {[Connection]: boolean}
	
	--[[
		Remove the connection from the table.
		This is safe because we're not iterating over the table,
		and we don't need to worry about rehashing
	]]--
	
	if not connections then return end
	connections[self] = nil
end

function StreamConnection:__call()
	self:Disconnect()
end

-- StreamEvent Class (Main)
local StreamEvent = {}
StreamEvent.__index = StreamEvent

-- Constructs a new StreamEvent instance
function StreamEvent.new()
	return setmetatable({
		_connections = {} :: {[Connection]: boolean}
	}, StreamEvent)
end

-- Connects a function to the stream
function StreamEvent:Connect(fn: (...any) -> ()): Connection
	local connection = StreamConnection.new(self, fn)
	
	self._connections[connection] = true
	
	return connection
end

-- Automatically disconnects after the first stream fire
function StreamEvent:Once(fn: (...any) -> ()): Connection
	local connection: Connection
	local fired = false

	connection = self:Connect(function(...)
		if fired then return end
		fired = true
		
		connection:Disconnect()
		connection = nil :: any
		
		fn(...)
	end)

	return connection
end

-- Yields the thread until the next stream fire, then returns the yielded coroutine
function StreamEvent:Wait(): ...any
	local runningThread = coroutine.running()
	local connection: Connection

	connection = self:Connect(function(...)
		connection:Disconnect()
		
		task.spawn(runningThread, ...)
	end)

	return coroutine.yield()
end

-- Fires the stream, calling all connected functions with the given arguments
function StreamEvent:Fire(...: any)
	-- Dictionary iteration is perfectly safe even if a connection 
	-- disconnects itself during the execution loop.
	local connections = self._connections :: {[Connection]: boolean}
	if not connections then
		return
	end
	
	for connection in pairs(connections) do
		if connection.Connected then
			task.spawn(connection._fn, ...)
		end
	end
end

-- Disconnects all connections from the stream
function StreamEvent:DisconnectAll()
	local connections = self._connections :: {[Connection]: boolean}
	if not connections then return end

	-- Fast-invalidate all connections without mutating the table mid-loop
	for connection: Connection in pairs(connections) do
		connection.Connected = false
	end
	
	table.clear(connections)
end

-- Disconnects all connections and destroys the stream instance
function StreamEvent:Destroy()
	self:DisconnectAll()
	self._connections = nil :: any
	
	setmetatable(self, nil)
end

return StreamEvent
