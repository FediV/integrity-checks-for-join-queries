import json


def create_proportions_list(r_size: int, values: list) -> list:
    # values (value, percentage)
    cardinalities = []
    for value in values:
        assert 0 <= value[1] <= 1
        cardinalities.append((value[0], round(value[1] * r_size)))
    cardinality_1 = r_size - sum([round(card[1]*r_size*card[0]) for card in cardinalities])
    value_1 = 1 / r_size
    cardinalities.append((value_1, cardinality_1))
    res = []
    for card in cardinalities:
        res.extend([card[0]] * int(card[1]))
    return res


if __name__ == "__main__":
    with open("../db.config.json", "r") as file:
        data = json.load(file)
    #resL = create_proportions_list(int(data["dbs"]["L"]["numberOfTuples"]), [(0.005, 0.0001), (0.003, 0.0001),
    #                                                                         (0.002, 0.0001), (0.001, 0.0002),
    #                                                                         (0.0005, 0.001), (0.0003, 0.002),
    #                                                                         (0.0002, 0.003)])
    resR = create_proportions_list(int(data["dbs"]["R"]["numberOfTuples"]), [(0.002, 0.5)])
    #data["dbs"]["L"]["joinProportion"] = resL
    data["dbs"]["R"]["joinProportion"] = resR
    print(sum(data["dbs"]["R"]["joinProportion"]))
    with open("../db.config.json", "w") as file:
        json.dump(data, file, indent=4)
