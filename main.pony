use "collections"
use "random"
use "math"
use "time"

actor GossipWorker
    var env: Env
    var id: U64
    var numRumors: U64
    var numNodes: U64

    new create(env': Env, id': U64, numNodes': U64) => 
        env = env'
        id = id'
        numRumors = 0
        numNodes = numNodes'

    be receive_message(message: String, coordinator: Coordinator) => 
        numRumors = numRumors + 1
        if(numRumors == 1) then
            send_message(message, coordinator)
            coordinator.increment_infectedNodes_counter()
        end

    
    be send_message(message: String, coordinator: Coordinator) => 
        if numRumors < 10 then
            coordinator.send_message_middleware_gossip(id, message, coordinator)
            coordinator.check_terminated_neighbours_gossip(id, message, coordinator)
            send_message(message, coordinator)
        else
            // env.out.print("Process Over" + id.string())
            coordinator.terminate_node(id)
            coordinator.decrement_infectedNodes_counter()
        end

actor PushSumWorker
    var env: Env
    var id: U64
    var numNodes: U64
    var s: F64
    var w: F64
    var change1: F64 = -1
    var change2: F64 = -1
    var change3: F64 = -1
    var ratio: F64 = -1
    var threshold: F64 = 1e-10
    var receivedMessage: Bool = false

    new create(env': Env, id': U64, numNodes': U64) => 
        env = env'
        id = id'
        numNodes = numNodes'
        s = id.f64()
        w = 1
    
    be receive_message(s1: F64, w1: F64, coordinator: Coordinator) =>
        s = s + s1
        w = w + w1
        if ratio == -1 then
            ratio = s/w
        else    
            let newRatio = s/w
            if change1 == -1 then
                if ratio == 0 then
                    change1 = 0
                else
                    change1 = ((newRatio-ratio)/ratio).abs()
                end
            elseif change2 == -1 then
                if ratio == 0 then
                    change2 = 0
                else
                    change2 = ((newRatio-ratio)/ratio).abs()
                end
            elseif change3 == -1 then
                if ratio == 0 then
                    change3 = 0
                else
                    change3 = ((newRatio-ratio)/ratio).abs()
                end
            else
                change1 = change2
                change2 = change3
                if ratio == 0 then
                    change3 = 0
                else
                    change3 = ((newRatio-ratio)/ratio).abs()
                end
            end
            ratio = newRatio
        end

        if receivedMessage == false then
            send_message(coordinator)
            coordinator.increment_infectedNodes_counter()
            receivedMessage = true
        end

    
    be send_message(coordinator: Coordinator) =>
        if (change1 != -1) and (change2 != -1) and (change3 != -1) then
            if (change1 < threshold) and (change2 < threshold) and (change3 < threshold) then
                // env.out.print("Process Over" + id.string() + "  Ratio " + ratio.string() + "  Change " + change3.string())
                coordinator.terminate_node(id)
                coordinator.decrement_infectedNodes_counter()
            else 
                s = s/2
                w = w/2
                coordinator.send_message_middleware_pushSum(id, s, w, coordinator)
                coordinator.check_terminated_neighbours_pushSum(id, 0, 0 , coordinator)
                send_message(coordinator)
            end
        else 
            s = s/2
            w = w/2
            coordinator.send_message_middleware_pushSum(id, s, w, coordinator)
            coordinator.check_terminated_neighbours_pushSum(id, 0, 0 , coordinator)
            send_message(coordinator)
        end
        

actor Coordinator
    var env: Env
    var workersListGossip: Array[GossipWorker]
    var workersListPushSum: Array[PushSumWorker]
    var numNodes: U64
    var topology: String
    var algorithm: String
    var graph: Array[Array[U64]] = Array[Array[U64]]
    var rng: Rand = Rand
    var terminatedNodes: Array[Bool]
    var numNodesRow: U64
    var numNodesInfected: U64 = 0
    var startTime: (I64, I64)

    new create(env': Env, numNodes': U64, topology': String, algorithm': String) =>
        env = env'
        workersListGossip = Array[GossipWorker]
        workersListPushSum = Array[PushSumWorker]
        numNodes = numNodes'
        topology = topology'
        algorithm = algorithm'
        terminatedNodes = Array[Bool]
        startTime = Time.now()

        var numNodesRowF64 = (numNodes.f64()).pow(1.0 / 3.0)
        numNodesRow = numNodesRowF64.u64()

        for i in Range[U64](0, numNodes) do
            if algorithm == "gossip" then
                let gossipWorker: GossipWorker = GossipWorker(env, i, numNodes)
                workersListGossip.push(gossipWorker)
            else 
                let pushSumWorker: PushSumWorker = PushSumWorker(env, i, numNodes)
                workersListPushSum.push(pushSumWorker)
            end
            terminatedNodes.push(false)
        end


        if topology == "full" then
            for i in Range[U64](0, numNodes) do
                let row: Array[U64] = Array[U64]
                for j in Range[U64](0,numNodes) do
                    if(j != i) then
                        row.push(j)
                    end
                end
                graph.push(row)
            end
        elseif topology == "line" then
            for i in Range[U64](0, numNodes) do
                let row: Array[U64] = Array[U64]
                if i == 0 then
                    row.push(1)
                elseif i == (numNodes-1) then
                    row.push(numNodes-2)
                else 
                    row.push(i-1)
                    row.push(i+1)
                end
                graph.push(row)
            end
        else

            var graph3D: Array[Array[Array[U64]]] = Array[Array[Array[U64]]]
            var actorNumber: U64 = 0

            for i in Range[U64](0, numNodesRow) do
                var temp2Dgraph: Array[Array[U64]] = Array[Array[U64]]
                for j in Range[U64](0, numNodesRow) do
                    var temp1Dgraph: Array[U64] = Array[U64]
                    for k in Range[U64](0, numNodesRow) do
                        temp1Dgraph.push(actorNumber)
                        actorNumber = actorNumber + 1
                    end
                    temp2Dgraph.push(temp1Dgraph)
                end
                graph3D.push(temp2Dgraph)
            end

            for i in Range[U64](0, numNodesRow) do
                for j in Range[U64](0, numNodesRow) do
                    for k in Range[U64](0, numNodesRow) do
                        var currentNode: U64
                        try 
                            currentNode = graph3D(i.usize())?(j.usize())?(k.usize())?
                        else continue end

                        let row: Array[U64] = Array[U64]
                        if (i>0) then
                            try
                                let neighId: U64 = graph3D((i-1).usize())?(j.usize())?(k.usize())?
                                row.push(neighId)
                            else continue end
                        end
                        if (i<(numNodesRow-1)) then
                            try
                                let neighId: U64 = graph3D((i+1).usize())?(j.usize())?(k.usize())?
                                row.push(neighId)
                            else continue end
                        end
                        if (j>0) then
                            try
                                let neighId: U64 = graph3D(i.usize())?((j-1).usize())?(k.usize())?
                                row.push(neighId)
                            else continue end
                        end
                        if (j<(numNodesRow-1)) then
                            try
                                let neighId: U64 = graph3D(i.usize())?((j+1).usize())?(k.usize())?
                                row.push(neighId)
                            else continue end
                        end
                        if (k>0) then
                            try
                                let neighId: U64 = graph3D(i.usize())?(j.usize())?((k-1).usize())?
                                row.push(neighId)
                            else continue end
                        end
                        if (k<(numNodesRow-1)) then
                            try
                                let neighId: U64 = graph3D(i.usize())?(j.usize())?((k+1).usize())?
                                row.push(neighId)
                            else continue end
                        end
                        if topology == "imp3D" then
                            var remainingNodes: Array[U64] = Array[U64]
                            for node in Range[U64](0, ((numNodesRow*numNodesRow)*numNodesRow)) do
                                var isNodeRemaining: Bool = true
                                for neighId in row.values() do
                                    if (node == neighId) then
                                        isNodeRemaining = false
                                    end
                                end
                                if (node == currentNode) then
                                    isNodeRemaining = false
                                end
                                if isNodeRemaining == true then
                                    remainingNodes.push(node)
                                end
                            end

                            var rand: U64 = rng.u64() % remainingNodes.size().u64()
                            try 
                                let neighId: U64 = remainingNodes(rand.usize())?
                                row.push(neighId)
                            else continue end

                        end
                        graph.push(row)
                    end
                end
            end                  
        end


    be start_gossip_process(message: String, coordinator: Coordinator) =>
        try
            workersListGossip(0)?.receive_message(message, coordinator)
        else 
            env.out.print("Some Error Occured 1!") 
        end
    
    be start_pushSum_process(s: F64, w: F64, coordinator: Coordinator) =>
        try
            workersListPushSum(0)?.receive_message(s, w, coordinator)
        else 
            env.out.print("Some Error Occured 2!") 
        end

    be send_message_middleware_gossip(id: U64, message: String, coordinator: Coordinator) => 
        try
            var numNeighbours: U64 = graph(id.usize())?.size().u64()
            var randomNum: U64 = rng.u64() % numNeighbours
            var neighbourId: U64 = graph(id.usize())?(randomNum.usize())?
            workersListGossip(neighbourId.usize())?.receive_message(message, coordinator)
        else
            env.out.print("Some Error Occured 3!")
        end
    
    be send_message_middleware_pushSum(id: U64, s: F64, w: F64, coordinator: Coordinator) => 
        try
            var numNeighbours: U64 = graph(id.usize())?.size().u64()
            var randomNum: U64 = rng.u64() % numNeighbours
            var neighbourId: U64 = graph(id.usize())?(randomNum.usize())?
            workersListPushSum(neighbourId.usize())?.receive_message(s, w, coordinator)
        else
            env.out.print("Some Error Occured 4!")
        end
    
    be terminate_node(id: U64) =>
        try
            terminatedNodes(id.usize())? = true
        else
            env.out.print("Some Error Occured 5")
        end

    be check_terminated_neighbours_gossip(id: U64, message: String, coordinator: Coordinator) => 
        try
            var shouldTerminate: Bool = true
            var neighboursArray:Array[U64] = graph(id.usize())?
            for neighbourId in neighboursArray.values() do
                shouldTerminate = shouldTerminate and terminatedNodes(neighbourId.usize())?
            end
            if shouldTerminate == true then
                workersListGossip(id.usize())?.receive_message(message, coordinator)
            end
        else
            env.out.print("Some Error Occured 6")
        end

    be check_terminated_neighbours_pushSum(id: U64, s: F64, w: F64, coordinator: Coordinator) => 
        try
            var shouldTerminate: Bool = true
            var neighboursArray:Array[U64] = graph(id.usize())?
            for neighbourId in neighboursArray.values() do
                shouldTerminate = shouldTerminate and terminatedNodes(neighbourId.usize())?
            end
            if shouldTerminate == true then
                workersListPushSum(id.usize())?.receive_message(s, w, coordinator)
            end
        else
            env.out.print("Some Error Occured 7")
        end
        
    be increment_infectedNodes_counter() =>
        numNodesInfected = numNodesInfected + 1
    
    be decrement_infectedNodes_counter() =>
        numNodesInfected = numNodesInfected - 1
        if(numNodesInfected == 0) then
            var endTime: (I64, I64) = Time.now()
            var timeDiff: F64 = (((endTime._1 - startTime._1)*1000000000) + (endTime._2 - startTime._2)).f64()/1000000000
            env.out.print(timeDiff.string())
        end
   

actor Main
    new create(env: Env) =>
        try 
            var numNodesString: String = env.args(1)?
            var topology: String  = env.args(2)?
            var algorithm: String = env.args(3)?
            var numNodes: U64 = numNodesString.u64()?

            var coordinator: Coordinator = Coordinator(env, numNodes, topology, algorithm)
            if algorithm == "gossip" then
                coordinator.start_gossip_process("Hello World", coordinator)
            else
                coordinator.start_pushSum_process(0,0, coordinator)
            end
        else
            env.out.print("Some Error Occured 8!")
        end


