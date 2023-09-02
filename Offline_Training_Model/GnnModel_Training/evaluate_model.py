import torch


def compute_accuracy(model,validation_dataset,dict_feats,nodes_with_neighbors):


    diffreces = []

    for query,view in validation_dataset.items():

        target = view['benefit']

        prediction = model(query,view['view'],3,nodes_with_neighbors,dict_feats)

        diff = prediction - target

        diffreces.append(torch.tensor(diff/target))

    #compute the MAPE mean absolute percentage error as accuracy

    MAPE = torch.mean(torch.stack(diffreces))

    return MAPE



def MV_maintenace_latency():

    latency = 0

    return latency



def compute_reduced_execution_time(workload_without_MVs,workload_with_MVs):

    total_reduced_time = 0

    return total_reduced_time

