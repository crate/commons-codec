from commons_codec.model import SQLParameterizedClause


def test_parameterized_clause_rval_set():
    clause = SQLParameterizedClause()

    container_column = "data"
    column = "foo"
    value = "bar"
    rval = f"CAST(:{column} AS OBJECT)"

    clause.add(lval=f"{container_column}['{column}']", name=column, value=value, rval=rval)

    assert clause == SQLParameterizedClause(
        lvals=["data['foo']"], rvals=["CAST(:foo AS OBJECT)"], values={"foo": "bar"}
    )


def test_parameterized_clause_rval_unset():
    clause = SQLParameterizedClause()

    container_column = "data"
    column = "foo"
    value = "bar"
    rval = None

    clause.add(lval=f"{container_column}['{column}']", name=column, value=value, rval=rval)

    assert clause == SQLParameterizedClause(lvals=["data['foo']"], rvals=[":foo"], values={"foo": "bar"})
