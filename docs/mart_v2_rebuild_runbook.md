# Mart v2 local rebuild

## Estado e escopo

Este runbook descreve a reconstrução local da camada canônica `mart_v2` e da
projeção pública `serving_v2`. A execução não acessa produção, não publica
imagem, não faz push, não restaura dump e não altera a origem `raw`.

Os artefatos de carga, logs, dbt e fingerprints ficam em
`D:\football-analytics-rebuild`; o SSD não é usado como diretório de trabalho
da reconstrução.

## Contrato de camadas

```text
raw/raw_src/raw_reference
        ↓  SQL de normalização, identidade e deduplicação
mart_v2  (dimensões, fatos, lineage, publicação/quarentena)
        ↓  dbt models/serving
serving_v2  (catálogo, perfis, partidas e busca)
        ↓
API v2 → frontend
```

`mart_v2` é a fonte canônica dos dados aceitos. `serving_v2` é derivada e
reconstruível; a API usa somente `mart_v2`/`serving_v2` quando
`BFF_DATA_LAYER=serving_v2`. O modo `legacy` continua disponível para
rollback local.

O produto Copa do Mundo não é uma segunda base: seus jogos, fases, grupos,
classificações, gols, elencos, seleções, rankings e finais são lidos de
`mart_v2`. O adaptador da API preserva as particularidades editoriais do
produto, mas não inventa campos ausentes: uma final empatada sem desempate
publicado permanece com vencedor desconhecido. Assets disponíveis em
`serving_v2.team_profile` continuam expostos no contrato visual.

## Execução

```powershell
.\tools\run_mart_v2_pipeline.ps1 -RunKey mart-v2-local-a
.\tools\validate_mart_v2.ps1 -RunKey mart-v2-local-a
```

O pipeline executa as fases SQL 001–007, materializa as oito tabelas de
`serving_v2` com dbt, roda 42 testes de contrato e executa a validação 009,
incluindo a matriz de cobertura e fingerprints físicos/lógicos.

O `tools\rebuild_mart_v2.ps1` sem `-SkipServing` permanece como caminho SQL
standalone para diagnóstico local; o pipeline oficial usa dbt para a
materialização final, evitando duas transformações concorrentes.

## Critérios de aceite

- zero `mart_v2.match_source` pendente;
- toda diferença entre referência e publicação classificada por motivo;
- fatos publicados com identidade canônica e referências válidas;
- 85 fingerprints físicos/lógicos presentes no run validado;
- duas execuções comparáveis com fingerprints idênticos;
- contratos dbt verdes e API/frontend verdes;
- rotas públicas de Copa do Mundo verificadas: hub, edição, rankings, finais,
  seleções e detalhe de seleção;
- busca p95 abaixo de 300 ms e perfil p95 abaixo de 500 ms no smoke local
  aquecido.

## Evidência atual

O candidato local validado tem 311.271 fatos de partida, 261.631 publicados e
49.640 quarentenados. A matriz fecha o delta publicado de 12.778 partidas:
49.190 por identidade de clube não resolvida, 274 sem data e 176 por candidato
semântico duplicado. A medição de busca está em
`D:\football-analytics-rebuild\benchmarks\search-slo-mart-v2-final-b3.json`.

As execuções comparáveis finais foram `mart-v2-final-a3` (run id 5) e
`mart-v2-final-b3` (run id 6). Ambas fecharam em
`248.853 / 311.271 / 261.631 / 49.640 / 0` para referência, observadas,
publicadas, quarentenadas e pendentes; a matriz de cobertura e a matriz de
razões tiveram diferença `0` entre as execuções, e cada run contém 85
fingerprints. Os arquivos estão em
`D:\football-analytics-rebuild\fingerprints\mart-v2-5.txt` e
`D:\football-analytics-rebuild\fingerprints\mart-v2-6.txt`.

No smoke frio, `/health` pode responder `503 degraded` enquanto o pool abre a
primeira conexão; depois do aquecimento, `/health` e todas as rotas públicas
verificadas responderam `200`. Isso é o estado de prontidão do ambiente local,
não uma troca de dados ou fallback para a camada legada.

O smoke visual final confirmou as rotas `/copa-do-mundo`,
`/copa-do-mundo/2022`, `/copa-do-mundo/rankings`,
`/copa-do-mundo/finais` e `/copa-do-mundo/selecoes/3000000001213`. A edição
2022 expõe 64 partidas, oito grupos e cinco fases eliminatórias; a final
permanece 3–3 porque a mart não contém o desempate correspondente.

O proxy BFF local mantém GETs públicos em cache. Depois de alterar o contrato
da API, recrie ou invalide o runtime do frontend antes do smoke para não
confundir resposta antiga em cache com regressão de dados.

## Rollback

Para voltar ao comportamento anterior, iniciar a API com
`BFF_DATA_LAYER=legacy`. Isso não apaga `mart_v2`, `serving_v2` nem o banco
antigo; a remoção/cutover de produção exige uma decisão posterior e uma
janela própria.
