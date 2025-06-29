--1) - Frequência de ocorrência de um valor específico.

DO $$
DECLARE
  cur_ref   REFCURSOR;
  rec RECORD;
  slct TEXT;
  top_5 INT := 5;  -- quantos itens queremos
BEGIN
  -- monta a query dinâmica pegando o nome direto da tabela de fatos
  slct := format(
    'SELECT 
       f.id_produto,
       MAX(f.nome_produto) AS nome_produto,
       SUM(f.qtd_produto) AS total_vendido
     FROM dw.fato_pedidos_rk f
     GROUP BY f.id_produto
     ORDER BY total_vendido DESC
     LIMIT %s',
    top_5
  );

  OPEN cur_ref FOR EXECUTE slct;

  RAISE NOTICE 'Top % produtos mais vendidos:', top_5;
  LOOP
    FETCH cur_ref INTO rec;
    EXIT WHEN NOT FOUND;
    RAISE NOTICE 'Produto % (ID: %) → total vendido: %',
                 rec.nome_produto, rec.id_produto, rec.total_vendido;
  END LOOP;

  CLOSE cur_ref;
END;
$$ LANGUAGE plpgsql;

-- 2) - trigger, criação de log para a tabela pedidos.
 -- Criação da tabela de log
CREATE TABLE IF NOT EXISTS log_pedidos (
    id SERIAL PRIMARY KEY,
    pedido_id BIGINT,
    cliente_id BIGINT,
    data_log TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Função que registra o log após a inserção
CREATE OR REPLACE FUNCTION registrar_log_pedido()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO log_pedidos (pedido_id, cliente_id)
    VALUES (
        NEW.id_pedido,
        NEW.id_cliente
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger para acionar a função após inserção na tabela fato_pedidos
CREATE TRIGGER trigger_log_pedido
AFTER INSERT ON fato_pedidos
FOR EACH ROW
EXECUTE FUNCTION registrar_log_pedido();
