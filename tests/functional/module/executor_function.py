from pipetree.artifact import Item

def stageB(StageA):
    yield Item(
        type="list",
        payload=StageA[0].payload['parameter_a'],
        meta={"list_length": len(StageA[0].payload['parameter_a'])})
    yield Item(
        type="list",
        payload=list(reversed(StageA[0].payload['parameter_a'])),
        meta={"list_length": len(StageA[0].payload['parameter_a'])})
    yield Item(
        type="information",
        payload="information_value",
        meta={"importance": 5.0}
    )

def stageC(StageB):
    lists = list(map(lambda x: x.item.payload, StageB['list']))
    infos = StageB['information']

    yield Item(
        type="analysis",
        payload=", ".join(lists[0]) + " " + infos[0].item.payload + str(infos[0].item.meta['importance'])
    )
