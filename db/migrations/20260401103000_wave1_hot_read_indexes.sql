-- migrate:up
-- Intencionalmente sem operacao.
-- Estes indices pertencem a superficies derivadas do dbt e devem ser geridos
-- no ciclo de materializacao dos modelos, nao no bootstrap de schema base.
SELECT 1;

-- migrate:down
SELECT 1;
