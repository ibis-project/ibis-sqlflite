import ibis
from ibis import _


def test_ibis_basics(con):
    t = con.table('lineitem')

    # use the Ibis dataframe API to run TPC-H query 1
    expr = (t.filter(_.l_shipdate.cast('date') <= ibis.date('1998-12-01') + ibis.interval(days=90))
            .mutate(discount_price=_.l_extendedprice * (1 - _.l_discount))
            .mutate(charge=_.discount_price * (1 + _.l_tax))
            .group_by([_.l_returnflag,
                       _.l_linestatus
                       ]
                      )
            .aggregate(
        sum_qty=_.l_quantity.sum(),
        sum_base_price=_.l_extendedprice.sum(),
        sum_disc_price=_.discount_price.sum(),
        sum_charge=_.charge.sum(),
        avg_qty=_.l_quantity.mean(),
        avg_price=_.l_extendedprice.mean(),
        avg_disc=_.l_discount.mean(),
        count_order=_.count()
    )
            .order_by([_.l_returnflag,
                       _.l_linestatus
                       ]
                      )
            )

    results = expr.execute()
    print(results)
    assert results is not None
