# ROADMAP OPERACIONAL — COMPETITION MODEL EVOLUTION
**Projeto:** football-analytics  
**Referência:** 2026-03-25  
**Natureza:** Plano de implementação técnico — não é brainstorming

---

## PRINCÍPIOS DE EXECUÇÃO

Estes princípios regem todas as decisões de implementação. Nenhum bloco pode ser aprovado se violar qualquer um deles.

**1. Expansão aditiva, nunca substituição destrutiva**  
Toda entidade nova entra como campo nullable ou tabela nova. Nada que já funciona para ligas pode ser alterado de forma que quebre o comportamento atual antes de a camada nova estar validada.

**2. Comportamento dirigido por metadado, nunca por nome de competição**  
Nenhum branch de código pode usar `competition_key` como condição lógica. O sistema deve derivar o comportamento de `stage_format` e `season_format_code`. Isso é verificável e é critério de recusa em revisão.

**3. Sem hardcode de regra disperso e sem configuração-deus**  
Regras precisam ser centralizadas por boundary correto, não espalhadas em código e nem concentradas indevidamente em uma única configuração.  
`competition_season_config` existe para identidade e formato macro da edição; semântica de fase, progressão, apresentação e exceções regulamentares precisam ter fronteiras próprias.

**4. Ligas continuam verdes em qualquer ponto do roadmap**  
Qualquer bloco que cause regressão em ligas é bloqueado imediatamente. Não existe exceção.

**5. Pilotos fechados antes de expansão**  
A ordem `copa_do_brasil` → `libertadores` → `champions_league` não é sugestão. É a sequência mínima de risco. Nenhum bloco de expansão começa antes do piloto anterior ter passado nas validações obrigatórias.

**6. Estrutura derivada precisa ser auditável**  
Quando `tie`, `group`, `leg` ou `progression` não estiverem explicitamente representados na origem disponível, a derivação pode existir, mas precisa ser rastreável, marcada e semanticamente verificável. Estrutura derivada sem marcação é estrutura incorreta.

**7. Separação estrita dos quatro eixos conceituais**  
- Taxonomia de portfólio (navegação/produto): `domestic_league`, `domestic_cup`, `international_cup`  
- Formato da competição-temporada: `league`, `knockout`, `hybrid`  
- Formato da fase: `league_table`, `group_table`, `knockout`, `qualification_knockout`, `placement_match`  
- Escopo de participante: `club`, `national_team`  

Esses eixos não se misturam. Um eixo nunca substitui outro.

**8. Renomeação de `league_*` → `competition_*` é etapa separada e posterior**  
Essa migração de nomenclatura não pode ocorrer durante nenhum dos blocos de habilitação de copa. Se acontecer junto, o risco de regressão é alto e a rastreabilidade cai.

**9. Evidência ancorada em temporadas-prova do acervo atual**  
Cada piloto central precisa fixar temporadas-prova no Bloco 0 e mantê-las até o fechamento do piloto.  
Para `champions_league`, as temporadas mínimas obrigatórias são `2023/24` e `2024/25`.  
Para `copa_do_brasil` e `libertadores`, as duas temporadas-prova precisam ser escolhidas entre as edições já disponíveis no acervo atual do projeto.

---

## CONTEXTO OPERACIONAL DESTA FRENTE

Este roadmap parte do estado real do projeto, não de um sistema abstrato.

- Ligas já são baseline estável do projeto em modelagem, contratos e produto. Esta frente não existe para reabrir liga; existe para expandir o modelo atual de forma aditiva.
- A fundação nova precisa se acoplar ao que o projeto já tem hoje como base de partida: `dim_stage`, `fact_matches`, `fact_standings_snapshots`, contratos de liga e navegação de liga já verdes.
- O sucesso desta frente é definido pelo fechamento ponta a ponta de três pilotos centrais: `copa_do_brasil`, `libertadores` e `champions_league`.
- FIFA permanece no roadmap apenas como expansão posterior planejada para evitar retrabalho estrutural. Não tem prioridade equivalente aos três pilotos centrais nesta fase.
- Analytics avançados, edge cases e renomeações amplas permanecem posteriores à estabilização dos pilotos centrais.

### Prioridade operacional desta frente

1. Fundação obrigatória para suportar os pilotos centrais: Blocos 0, 1A, 1B e 1C.
2. Fechamento dos pilotos centrais em dados e estrutura: Blocos 2, 3A, 3B e 4.
3. Fechamento ponta a ponta em contratos e produto: Blocos 5A e 5B.
4. Expansão posterior para evitar retrabalho futuro: Bloco 6.
5. Escopo posterior, fora da definição de sucesso desta frente: Blocos 7 e 8.

---

## CONTRATOS SEMÂNTICOS DO MODELO

Esta seção define o vocabulário mínimo obrigatório do modelo. Cada conceito existe para resolver um problema semântico específico e não pode ser usado como atalho para outro.

### `competition`

- **Representa:** a identidade canônica e estável de um torneio como produto e catálogo.
- **Existe quando:** o sistema precisa reconhecer uma mesma competição ao longo de várias edições.
- **Depende de:** um catálogo canônico de competições.
- **Nunca deve representar:** uma edição específica, uma fase, um grupo, um confronto ou uma regra de progressão.
- **Invariantes semânticos:** mantém identidade pública estável; não carrega sozinho a regra da edição; não mistura escopos incompatíveis de participante.

### `competition season`

- **Representa:** uma edição oficial de uma competição em um recorte temporal específico.
- **Existe quando:** a competição é instanciada para uma temporada, ano, ciclo ou edição.
- **Depende de:** uma `competition`.
- **Nunca deve representar:** o detalhamento de todas as fases, toda a progressão da edição ou a navegação de produto.
- **Invariantes semânticos:** pertence a exatamente uma `competition`; define a família estrutural da edição; pode mudar de formato entre edições sem quebrar a identidade pública da competição.

### `stage`

- **Representa:** uma fase estrutural da edição com papel esportivo próprio.
- **Existe quando:** a edição possui pelo menos uma fase semanticamente distinta.
- **Depende de:** uma `competition season`.
- **Nunca deve representar:** a edição inteira, um grupo, um confronto, um leg ou apenas uma convenção visual.
- **Invariantes semânticos:** pertence a uma única edição; possui tipo semântico claro; sua ordem estrutural é estável dentro da edição.

### `group`

- **Representa:** uma partição competitiva dentro de uma fase com múltiplas tabelas paralelas.
- **Existe quando:** o `stage` é orientado a grupos.
- **Depende de:** um `stage` elegível para agrupamento.
- **Nunca deve representar:** a fase inteira, um confronto eliminatório ou um agrupamento editorial.
- **Invariantes semânticos:** pertence a exatamente um `stage`; standings dentro do grupo só comparam participantes desse mesmo grupo; não substitui progressão.

### `tie`

- **Representa:** a unidade eliminatória de confronto entre dois lados competitivos.
- **Existe quando:** o `stage` é eliminatório ou possui confrontos que determinam classificação ou eliminação.
- **Depende de:** um `stage` eliminatório.
- **Nunca deve representar:** uma partida individual, a chave inteira ou uma simples relação potencial entre times.
- **Invariantes semânticos:** pertence a exatamente um `stage`; tem dois lados competitivos; sua resolução determina um resultado competitivo verificável.

### `leg`

- **Representa:** uma partida específica vinculada a um `tie`.
- **Existe quando:** um confronto eliminatório é disputado em uma ou mais partidas.
- **Depende de:** um `tie` e uma `match`.
- **Nunca deve representar:** o agregado do confronto, a fase inteira ou a progressão entre fases.
- **Invariantes semânticos:** uma `match` pertence a no máximo um `tie`; `leg` tem ordem identificável; resultado de `tie` não se reduz ao resultado bruto de um único `leg` quando houver múltiplos.

### `progression`

- **Representa:** a relação semântica entre um resultado competitivo de origem e uma entrada estrutural no destino.
- **Existe quando:** a edição possui múltiplas fases ou precisa encaminhar classificados, vencedores ou eliminados.
- **Depende de:** uma `competition season`, um `stage` de origem e um destino estrutural.
- **Nunca deve representar:** apenas ordenação cronológica de fases, um link visual de bracket ou uma regra de desempate interna.
- **Invariantes semânticos:** é direcional; é scoped à edição; liga resultado competitivo a destino estrutural.

### `standings context`

- **Representa:** o domínio exato em que participantes podem ser ranqueados por tabela.
- **Existe quando:** um `stage` admite classificação por tabela.
- **Depende de:** `competition season`, `stage` e, quando aplicável, `group`.
- **Nunca deve representar:** bracket eliminatório, ranking global artificial ou substituto de progressão.
- **Invariantes semânticos:** só compara participantes do mesmo escopo competitivo; pode ser `single_table`, `grouped_table` ou `not_applicable`.

### `bracket context`

- **Representa:** a estrutura consumível de confrontos e slots de uma fase eliminatória.
- **Existe quando:** um `stage` é eliminatório.
- **Depende de:** `stage`, `tie` e, quando aplicável, `leg` e `progression`.
- **Nunca deve representar:** tabela classificatória, ranking por pontos ou lista plana de partidas sem relação estrutural.
- **Invariantes semânticos:** organiza confrontos por relação estrutural; expõe vencedores e destinos; não induz semântica de standings.

### `league format`

- **Representa:** uma família estrutural em que a resolução principal vem de tabela.
- **Existe quando:** a edição é decidida predominantemente por acumulação e ranking em um mesmo escopo classificatório.
- **Depende de:** `standings context`.
- **Nunca deve representar:** uma edição híbrida ou eliminatória.
- **Invariantes semânticos:** a classificação principal vem de tabela; mata-mata eventual posterior descaracteriza a edição como `hybrid`.

### `cup format`

- **Representa:** uma família estrutural em que a resolução principal vem de confrontos eliminatórios.
- **Existe quando:** a edição é decidida por `tie` ou estrutura eliminatória equivalente.
- **Depende de:** `bracket context`.
- **Nunca deve representar:** tabela como mecanismo principal de decisão.
- **Invariantes semânticos:** progressão é determinada por confronto; standings, se existirem, são auxiliares ou inexistentes.

### `hybrid format`

- **Representa:** uma família estrutural em que a mesma edição combina, em fases diferentes, tabela e mata-mata.
- **Existe quando:** a edição possui pelo menos uma fase classificatória por tabela e uma fase eliminatória.
- **Depende de:** segmentação explícita por `stage` e ligação por `progression`.
- **Nunca deve representar:** um `stage` individual ou um caso tratado por nome de competição.
- **Invariantes semânticos:** o comportamento muda por fase, não por nome da competição; `standings context` e `bracket context` coexistem na mesma edição sem se confundir.

---

## SEPARAÇÃO DE RESPONSABILIDADES DE CONFIGURAÇÃO

O plano precisa de configuração, mas com boundaries explícitos. Não existe espaço para uma "tabela deus" conceitual.

### 1. Configuração de competição-temporada

Responsabilidade:

- identidade da edição;
- família estrutural da edição;
- escopo de participante;
- capacidades macro da edição.

Não deve absorver:

- semântica detalhada de fase;
- progressão entre fases;
- defaults de navegação;
- exceções regulamentares granulares.

### 2. Configuração de fase

Responsabilidade:

- tipo semântico do `stage`;
- elegibilidade para `standings context`;
- elegibilidade para `bracket context`;
- presença de grupos, confrontos e legs;
- ordenação estrutural da fase.

Não deve absorver:

- identidade global da edição;
- grafo de progressão completo;
- configuração de produto.

### 3. Configuração de progressão entre fases

Responsabilidade:

- definir como saídas competitivas de uma origem alimentam slots do destino;
- separar progressão estrutural de regra interna de classificação.

Não deve absorver:

- tipo da fase;
- regra de ranking da tabela;
- política de apresentação.

### 4. Configuração de apresentação/navegação de produto

Responsabilidade:

- definir defaults de consumo de produto;
- selecionar superfícies prioritárias por contexto (`standings`, `groups`, `bracket`, `calendar`);
- organizar a navegação sem reescrever o domínio.

Não deve absorver:

- regra esportiva;
- progressão;
- desempate;
- classificação estrutural da edição.

### 5. Exceções regulamentares específicas

Responsabilidade:

- registrar desvios pontuais e versionados que não cabem no contrato base.

Não deve absorver:

- a regra padrão da competição;
- a configuração principal da edição;
- a configuração principal de fase ou produto.

### 6. Regra de boundary

Se uma mesma configuração passa a decidir:

- identidade da edição;
- tipo de fase;
- progressão;
- navegação de produto;
- exceções históricas;

então o modelo ficou conceitualmente errado, mesmo que continue executando.

---

## ESTRUTURA MÍNIMA QUE O PRODUTO DEVE CONSEGUIR CONSUMIR

O produto não pode depender apenas de `stage_format` isolado. A estrutura mínima consumível precisa existir no nível de domínio.

### 1. Envelope da edição

Mínimo obrigatório:

- identidade da `competition season`;
- família estrutural da edição;
- escopo de participante;
- indicação se a edição possui contextos de tabela, mata-mata ou ambos.

### 2. Catálogo ordenado de fases

Mínimo obrigatório:

- lista ordenada de `stages`;
- tipo semântico de cada fase;
- capacidade de cada fase para `standings context` e `bracket context`.

### 3. Contextos de tabela

Mínimo obrigatório:

- `standings context` explícito para fases elegíveis;
- identificação de tabela única versus múltiplos grupos;
- ausência explícita de standings quando a fase não for elegível.

### 4. Estrutura de grupos

Mínimo obrigatório quando houver grupos:

- lista de `groups` pertencentes ao `stage`;
- ligação entre grupo e seu `standings context`;
- identificação clara de que grupos paralelos não se misturam.

### 5. Estrutura de confrontos

Mínimo obrigatório quando houver mata-mata:

- `bracket context` por fase eliminatória;
- lista de `ties` ordenados;
- ligação entre `tie` e seus `legs`, quando existirem.

### 6. Progressão entre fases

Mínimo obrigatório:

- relação semântica entre origem e destino;
- indicação de quem avança, para onde avança e em que slot estrutural entra.

### 7. Alternância entre visões

Mínimo obrigatório:

- o domínio precisa informar quando o consumo principal é tabela;
- o domínio precisa informar quando o consumo principal é bracket;
- torneios híbridos precisam expor essa alternância como parte do contrato estrutural da edição.

---

## IMPACTOS ARQUITETURAIS POR CAMADA

Esta seção não descreve execução. Ela delimita impacto, dependência e risco de acoplamento.

### Ingestão e captura

- **Impacto arquitetural:** a camada precisa preservar identidade estrutural suficiente para edição, fase, grupo, confronto e leg quando esses conceitos existirem.
- **Exigência do modelo:** não assumir que todo torneio se resume a partida + rodada + tabela.
- **Compatibilidade obrigatória:** ligas continuam válidas mesmo sem entidades adicionais.

### Normalização semântica

- **Impacto arquitetural:** esta camada passa a ser a fronteira onde `group`, `tie`, `leg` e `progression` ganham forma canônica.
- **Exigência do modelo:** separar contexto classificatório de contexto eliminatório.
- **Risco de acoplamento:** colapsar confrontos e grupos em estruturas pensadas para liga.

### Armazenamento e catálogo canônico

- **Impacto arquitetural:** o sistema precisa distinguir armazenamento operacional de domínio canônico e de configuração.
- **Exigência do modelo:** expansão aditiva e boundaries claros entre configuração de edição, fase, progressão, produto e exceções.
- **Compatibilidade obrigatória:** evitar rename amplo nesta etapa.

### Modelagem analítica

- **Impacto arquitetural:** fatos e marts deixam de poder tratar standings e confronto como simples variações do mesmo agregado universal.
- **Exigência do modelo:** preservar o que continua correto para ligas e introduzir contextos próprios para grupos, confrontos e progressão.
- **Risco de acoplamento:** forçar um único mart a servir liga, grupo e mata-mata sem mudança de grain.

### APIs e BFF

- **Impacto arquitetural:** contratos precisam refletir contextos semânticos distintos, não só adicionar filtros a uma API de liga.
- **Exigência do modelo:** expor estrutura da edição, standings context, bracket context e progressão.
- **Compatibilidade obrigatória:** contratos de liga não podem quebrar.

### Frontend e produto

- **Impacto arquitetural:** produto precisa consumir estrutura tipada de domínio, não ramos manuais por competição.
- **Exigência do modelo:** alternar entre tabela e mata-mata por fase.
- **Risco de acoplamento:** transformar competição híbrida em fluxo único ou codificar comportamento por nome de competição.

---

## ESCOPO ESSENCIAL VS ESCOPO POSTERIOR

### Essencial nesta fase

Essencial para suportar `copa_do_brasil`, `libertadores` e `champions_league`:

- contratos semânticos explícitos do domínio;
- separação clara entre configuração de edição, fase, progressão, produto e exceção;
- estrutura mínima consumível para fases, grupos, confrontos, legs e progressão;
- distinção entre `league format`, `cup format` e `hybrid format`;
- compatibilidade retroativa com ligas por expansão aditiva.

### Não essencial nesta fase

Não precisa dirigir a fundação agora:

- cobertura aprofundada de regulamentos raros;
- catálogo completo de expansões FIFA;
- analytics comparativos avançados entre eras e formatos;
- refinamentos editoriais e visuais além da estrutura mínima.

### Posterior

Deve entrar apenas depois de a fundação estar estável:

- expansões amplas para competições adicionais;
- edge cases intercompetições;
- critérios multi-nível raros;
- consolidação ampla de nomenclatura;
- sofisticações de produto que dependem de todos os contextos anteriores estarem maduros.

---

## SEQUÊNCIA RECOMENDADA DE BLOCOS

Blocos `0` a `5B` formam o foco principal desta frente e definem sucesso real desta evolução no projeto atual.  
Blocos `6` a `8` permanecem no roadmap, mas são expansão posterior e não podem disputar prioridade com os pilotos centrais.

```
BLOCO 0 — Contrato semântico e matriz de regras        [PRÉ-REQUISITO ABSOLUTO]
    └── BLOCO 1A — Fundação: competition_season_config
    └── BLOCO 1B — Fundação: ampliação de dim_stage
    └── BLOCO 1C — Fundação: extensão aditiva de fact_matches e standings
            └── BLOCO 2 — Piloto knockout puro: copa_do_brasil
                    └── BLOCO 3A — Piloto grupos: libertadores fase de grupos
                            └── BLOCO 3B — Piloto híbrido completo: libertadores ponta a ponta
                                    └── BLOCO 4 — Champions League: versionamento por temporada
                                            └── BLOCO 5A — APIs e contratos de BFF
                                            └── BLOCO 5B — Impactos de frontend e produto
                                                    └── BLOCO 6 — Onboarding FIFA
                                                            └── BLOCO 7 — Analytics avançados
                                                            └── BLOCO 8 — Casos complexos e edge cases
```

---

## BLOCO 0 — Contrato Semântico e Matriz de Regras

**Classificação de prioridade:** fundação obrigatória dos pilotos centrais.

### Objetivo
Produzir e aprovar a fundação arquitetural que vai orientar todos os blocos seguintes: contratos semânticos, boundaries de configuração, estrutura mínima consumível e matriz de edição por competição-temporada, sempre partindo do baseline já estável de ligas do projeto.

### Escopo exato
Cobrir obrigatoriamente, em profundidade: `copa_do_brasil`, `libertadores`, `champions_league`.  
Cobrir, em nível de extensão posterior e identidade de catálogo: `fifa_world_cup`, `fifa_club_world_cup`, `fifa_intercontinental_cup`.  
Não cobrir neste bloco: detalhe de UI, parser, desenho operacional de ingestão, estratégia de coleta ou schema físico definitivo.

### Entregáveis
- Contrato semântico explícito para: `competition`, `competition season`, `stage`, `group`, `tie`, `leg`, `progression`, `standings context`, `bracket context`, `league format`, `cup format`, `hybrid format`.
- Matriz de responsabilidades separando o que pertence a:
  - configuração de competição-temporada;
  - configuração de fase;
  - configuração de progressão;
  - configuração de apresentação/navegação;
  - exceções regulamentares.
- Matriz por `competition + season` para os casos essenciais com:
  - `format_family`;
  - `season_format_code`;
  - `participant_scope`;
  - famílias obrigatórias de `stage`;
  - presença esperada de `standings context` e `bracket context`.
- Matriz específica da `champions_league` separando explicitamente temporadas anteriores e posteriores à mudança de formato.
- Definição das temporadas-prova obrigatórias dos pilotos centrais:
  - `champions_league`: `2023/24` e `2024/25`;
  - `copa_do_brasil`: duas temporadas disponíveis no acervo atual;
  - `libertadores`: duas temporadas disponíveis no acervo atual.
- Decisão arquitetural de escopo essencial versus posterior, evitando inflar a fundação com expansões não críticas.

### Dependências
Nenhuma dependência técnica. Este bloco depende de alinhamento entre domínio esportivo, modelagem e produto sobre os contratos mínimos do torneio.

### Riscos
- **Alto:** confundir configuração de edição com configuração de fase e progressão. Mitigação: aprovar boundaries antes de escrever qualquer linha de implementação.
- **Médio:** tratar `champions_league` como uma única regra estrutural apesar da mudança de formato. Mitigação: fechar a matriz de versionamento por temporada neste bloco.
- **Médio:** tentar resolver escopo futuro demais na fundação. Mitigação: separar explicitamente essencial agora, posterior depois.

### Critérios de conclusão
- O contrato semântico mínimo do modelo está fechado e aprovado.
- A separação de responsabilidades de configuração está explícita e sem sobreposição indevida.
- Toda edição dos pilotos essenciais tem `season_format_code` definido e justificado.
- Cada piloto central tem suas temporadas-prova explicitamente fixadas para validação até o fechamento ponta a ponta.
- `champions_league` tem versionamento por temporada explicitamente resolvido.
- O escopo essencial desta frente está separado das expansões posteriores.

### Validações obrigatórias
- Verificar que `competition season` não carrega sozinha semântica detalhada de fase, progressão e navegação.
- Verificar que `stage` não foi transformado em substituto de `group`, `tie` ou `progression`.
- Verificar que `standings context` e `bracket context` existem como conceitos distintos.
- Verificar que `champions_league` tem ao menos dois `season_format_code` cobrindo formatos diferentes.

---

## BLOCO 1A — Fundação: `competition_season_config`

**Classificação de prioridade:** fundação obrigatória dos pilotos centrais.

### Objetivo
Criar a camada de configuração de competição-temporada restrita ao que realmente pertence à edição: identidade, família estrutural e versionamento macro de formato.

### Escopo exato
Criação da entidade `competition_season_config` com campos de escopo macro da edição.  
Este bloco não deve absorver semântica detalhada de fase, progressão entre fases, apresentação de produto ou exceções regulamentares.  
Não inclui ainda join com fatos ou marts; inclui apenas a existência de uma configuração de edição consultável e com boundary correto.

### Entregáveis
- `competition_season_config` disponível com scope limitado à edição.
- Carga inicial validada para os pilotos essenciais; extensões posteriores podem existir com granularidade menor.
- Convenção estável para `season_format_code`.
- Testes de integridade garantindo:
  - unicidade por `competition_key + season_label`;
  - não nulidade dos campos macro obrigatórios;
  - valores de `format_family` e `participant_scope` dentro do domínio permitido.

### Dependências
- Bloco 0 aprovado e matriz entregue.

### Riscos
- **Alto:** `competition_season_config` virar configuração-deus. Mitigação: bloquear inclusão de semântica que pertence a fase, progressão, produto ou exceção.
- **Médio:** tentar resolver edge cases regulatórios antes de fechar o contrato macro da edição. Mitigação: manter exceções fora deste bloco.

### Critérios de conclusão
- Tabela existe, é consultável e está carregada com os dados da matriz do Bloco 0.
- Todos os testes de integridade passam.
- Nenhum pipeline existente foi alterado.

### Validações obrigatórias
- Consulta por `competition_key=champions_league` retorna ao menos dois registros com `season_format_code` distintos.
- Consulta por `participant_scope=national_team` retorna apenas entradas de `fifa_world_cup`.
- Verificar que campos de progressão, navegação de produto e exceções não foram embutidos indevidamente nesta entidade.
- Rerun completo do pipeline de ligas continua verde após a criação desta tabela.

---

## BLOCO 1B — Fundação: Ampliação de `dim_stage`

**Classificação de prioridade:** fundação obrigatória dos pilotos centrais.

### Objetivo
Incorporar à camada de fase os campos necessários para que o sistema saiba o papel semântico de cada `stage`, sem transformar `stage` em substituto de progressão ou de produto.

### Escopo exato
Adição dos seguintes campos ao `dim_stage` atual, todos nullable: `stage_format`, `stage_code`, `sort_order`, `standings_context_mode`, `bracket_context_mode`, `group_mode`, `leg_mode`, `elimination_mode`.  
Nenhum campo existente é removido ou renomeado.  
Nenhuma query downstream é alterada neste bloco.  
Este bloco não deve embutir a progressão entre fases dentro de `dim_stage`.

### Entregáveis
- `dim_stage` ampliada com os novos campos nullable.
- Backfill dos campos novos para os stages das competições-alvo dos pilotos, usando a matriz do Bloco 0 como fonte de verdade.
- Stages de ligas continuam com os novos campos como null — esse é o comportamento correto e esperado.
- Testes confirmando que queries existentes sobre `dim_stage` continuam retornando os mesmos resultados para filtros de liga.

### Dependências
- Bloco 1A concluído (a referência a `competition_season_config` nos stages de copa já pode ser feita aqui).

### Riscos
- **Médio:** campos de progressão tentarem entrar em `dim_stage` por conveniência. Mitigação: bloquear `next_stage_*`, `advances_*` e equivalentes neste boundary.
- **Baixo:** `stage_format` ambíguo em parte do acervo. Mitigação: manter explícito o que está indefinido, sem contaminar o contrato.

### Critérios de conclusão
- `dim_stage` tem os novos campos, todos nullable.
- Stages das competições-alvo dos pilotos têm `stage_format` preenchido.
- Nenhum teste existente regrediu.

### Validações obrigatórias
- Stages de ligas não têm `stage_format` preenchido de forma incorreta — ou ficam null ou ficam como `league_table`.
- Consulta filtrando `stage_format=knockout` retorna apenas stages de eliminatória.
- Consulta filtrando `stage_format=group_table` retorna apenas stages de fase de grupos.
- Verificar que progressão não foi modelada como coluna implícita de `stage`.

---

## BLOCO 1C — Fundação: Extensão Aditiva de `fact_matches` e `fact_standings_snapshots`

**Classificação de prioridade:** fundação obrigatória dos pilotos centrais.

### Objetivo
Adicionar os campos de contexto de copa às tabelas de fatos existentes, de forma totalmente aditiva e nullable, sem alterar o grain atual nem as queries de liga.

### Escopo exato
Em `fact_matches`: adição de `group_id` (nullable), `tie_id` (nullable), `leg_number` (nullable). A referência de `stage_id` ou `stage_sk` já existente deve ser verificada quanto à consistência com a nova `dim_stage` — se já estiver coerente, nada muda.  
Em `fact_standings_snapshots`: adição de `group_id` / `group_sk` (nullable). O grain atual de liga (`competition + season + stage + round + team`) continua intacto. O grain de grupo (`competition + season + stage + group + round + team`) é o grain correto para snapshots de grupo, mas só será populado a partir do Bloco 3.  
Não inclui ainda a criação das entidades `group` e `tie` — apenas os campos de foreign key nullable já existem nas tabelas de fato.

### Entregáveis
- `fact_matches` com os três campos novos nullable.
- `fact_standings_snapshots` com `group_id` nullable.
- Testes confirmando que registros de liga têm os novos campos como null.
- Testes confirmando que os grains atuais de liga continuam intactos.

### Dependências
- Blocos 1A e 1B concluídos.

### Riscos
- **Baixo:** Adição de campo nullable em tabela de fatos grande pode impactar tempo de build. Mitigação: verificar antes de fazer merge em produção.

### Critérios de conclusão
- Todos os campos novos existem nas tabelas, são nullable e não alteram o comportamento atual.
- Pipeline completo de ligas roda sem erro e sem alteração de resultado.

### Validações obrigatórias
- Rerun idempotente da pipeline de ligas continua verde.
- Queries existentes sobre standings de ligas retornam exatamente os mesmos resultados antes e depois da migração.
- Contagem de linhas em `fact_matches` e `fact_standings_snapshots` permanece inalterada após a migração de schema.

---

## BLOCO 2 — Piloto Knockout Puro: `copa_do_brasil`

**Classificação de prioridade:** piloto central obrigatório.

> **Este é o piloto de menor complexidade. Aqui o modelo eliminatório puro é provado pela primeira vez.**

### Objetivo
Implementar e validar completamente o modelo de confronto eliminatório usando `copa_do_brasil` como caso de piloto, provando que `tie`, `fact_tie_results` e progressão básica funcionam antes de qualquer caso híbrido.

### Escopo exato
Criação da entidade `tie` com todos os campos definidos no modelo alvo: `tie_id`, `competition_key`, `season_label`, `stage_id`, `home_side_team_id`, `away_side_team_id`, `tie_order`, `winner_team_id`, `resolution_type`, `is_inferred`.  
Criação do mart `fact_tie_results` com agregação por confronto: gols totais por lado, vencedor, tipo de resolução.  
Implementação da lógica de derivação auditável de `tie` quando a estrutura de confronto não existir de forma nativa no recorte disponível — com `is_inferred=true` obrigatório nesses casos.  
Progressão básica: identificar o time classificado de cada confronto e sua fase de destino.  
Escopo restrito a `copa_do_brasil`. Nenhuma outra competição entra neste bloco.

### Entregáveis
- Entidade canônica de `tie` consultável com os campos especificados.
- `fact_tie_results` populado para as temporadas disponíveis no acervo.
- Lógica de derivação auditável de `tie` documentada e marcada com `is_inferred`.
- Progressão básica de classificados funcionando para ao menos uma temporada completa da `copa_do_brasil`.

### Dependências
- Blocos 1A, 1B e 1C concluídos e validados.
- Bloco 0 com `copa_do_brasil` documentada na matriz e com as regras estruturais mínimas do confronto definidas.

### Riscos
- **Alto:** a estrutura de confronto não estar explicitamente representada no recorte disponível. Mitigação: derivação auditável e semanticamente rastreável.
- **Médio:** Copa do Brasil tem fases com formato irregular (ex: jogo único nas primeiras fases). Mitigação: documentar na matriz de regras antes de implementar — não descobrir durante a implementação.
- **Baixo:** Confrontos com resolução por W.O. ou decisão administrativa. Mitigação: `resolution_type=administrative` já previsto no modelo.

### Critérios de conclusão
- `fact_tie_results` tem dados consistentes para as duas temporadas-prova de `copa_do_brasil` definidas no Bloco 0.
- Todo confronto completo tem exatamente um `winner_team_id` não nulo.
- Gols agregados em `fact_tie_results` batem com a soma dos resultados das partidas correspondentes em `fact_matches`.
- A progressão básica dos classificados fecha corretamente nas mesmas temporadas-prova.
- Confrontos inferidos estão marcados como `is_inferred=true`.
- Nenhuma regressão em ligas.

### Validações obrigatórias
- Um confronto completo tem exatamente um vencedor oficial — zero ou dois vencedores é erro.
- Soma de gols do confronto confere com `fact_matches` filtrado pelo mesmo `tie_id`.
- `resolution_type` é coerente com o estado final do confronto (ex: se há pênaltis, o tipo não pode ser `aggregate`).
- Confrontos inferidos não têm `is_inferred=false`.
- Pipeline de ligas (Brasileirão, Premier League, etc.) continua rodando sem alteração.

---

## BLOCO 3A — Piloto de Grupos: `libertadores` Fase de Grupos

**Classificação de prioridade:** piloto central obrigatório.

> **Este bloco prova múltiplas tabelas paralelas na mesma competição-temporada.**

### Objetivo
Implementar e validar o modelo de grupos usando a fase de grupos da `libertadores`, provando que `group`, `fact_group_standings` e o scoping correto de standings funcionam antes de atacar o caso híbrido completo.

### Escopo exato
Criação da entidade `group` com os campos: `group_id`, `competition_key`, `season_label`, `stage_id`, `group_name`, `group_order`.  
Criação do mart `fact_group_standings` com grain `competition + season + stage + group + round + team`.  
Scoping correto em standings: times de grupos diferentes nunca aparecem no mesmo snapshot de tabela.  
Filtro `stageId + groupId` funcional na camada de dados (o contrato de API será fechado no Bloco 5A).  
Escopo restrito à fase de grupos da `libertadores`. A fase eliminatória da mesma competição entra no Bloco 3B.

### Entregáveis
- Entidade `group` criada e carregada para as temporadas de `libertadores` disponíveis no acervo.
- `fact_group_standings` com grain correto e dados validados.
- Confirmação de que `fact_standings_snapshots` com `group_id` preenchido gera partições corretas por grupo.
- Documentação do scoping de grupo para uso pelos blocos de API e frontend.

### Dependências
- Bloco 2 concluído e validado (o modelo de `tie` será necessário na Bloco 3B, mas os grupos podem ser implementados independentemente).
- Bloco 0 com `libertadores` documentada e com as regras mínimas de grupo definidas.

### Riscos
- **Alto:** o recorte estrutural disponível não separar claramente contextos de grupo. Mitigação: o bloco só é aceito se o scoping por `stage + group` ficar explicitamente demonstrável.
- **Médio:** Temporadas com número de grupos variável entre edições. Mitigação: `group_order` no modelo já suporta isso; não hardcode de quantidade de grupos.

### Critérios de conclusão
- `fact_group_standings` populado e validado para as duas temporadas-prova de `libertadores` definidas no Bloco 0.
- Nenhum time de Grupo A aparece em snapshot do Grupo B.
- Grain `competition + season + stage + group + round + team` tem unicidade garantida por teste.
- Posição final de cada time no grupo confere para as temporadas-prova selecionadas.
- Nenhuma regressão em ligas.

### Validações obrigatórias
- Não existe mistura de times de grupos diferentes no mesmo snapshot de standings.
- Grain único por `competition-season-stage-group-round-team` — duplicata é erro.
- Posições dentro de cada grupo batem com a regra configurada para aquele contexto classificatório.
- Ligas continuam verdes.

---

## BLOCO 3B — Piloto Híbrido Completo: `libertadores` Ponta a Ponta

**Classificação de prioridade:** piloto central obrigatório.

> **Este bloco fecha o primeiro torneio híbrido do portfólio, conectando grupos com eliminatórias.**

### Objetivo
Integrar grupos e confrontos eliminatórios na mesma competição-temporada, implementar a progressão de classificados e criar o mart `fact_stage_progression`, fechando `libertadores` como o primeiro caso híbrido completo.

### Escopo exato
Progressão de classificados da fase de grupos para a fase eliminatória: times que avançam dos grupos entram nos `tie` da fase seguinte.  
Criação da configuração de progressão como boundary separado da configuração de fase.  
Criação do mart `fact_stage_progression` com os campos mínimos: `competition_key`, `season_label`, `team_id`, `from_stage_id`, `to_stage_id`, `progression_type` (classificado, eliminado, repescagem — onde aplicável), `is_inferred`.  
Validação da coerência entre o que `fact_group_standings` indica como classificados e o que `fact_tie_results` mostra como participantes da fase seguinte.  
O hub estrutural mínimo da competição (mapa de fases e sua sequência) deve ser consultável neste ponto — como estrutura de dados, sem interface.

### Entregáveis
- Configuração de progressão explicitamente separada da configuração de fase.
- `fact_stage_progression` criado e carregado para `libertadores`.
- Coerência validada entre classificados nos grupos e participantes dos confrontos eliminatórios.
- Documentação da regra de progressão usada (quantos avançam por grupo, critério de seleção).
- Estrutura de fases da `libertadores` consultável via dados: sequência, formato de cada fase, quem progride.

### Dependências
- Bloco 2 concluído (modelo de `tie` validado).
- Bloco 3A concluído (modelo de grupo validado).

### Riscos
- **Médio:** Temporadas com repescagem entre fase de grupos e oitavas. Mitigação: `progression_type=repechage` já previsto; não tratar como eliminação.
- **Médio:** a progressão ser reabsorvida indevidamente por `stage` ou por regra de produto. Mitigação: manter boundary explícito e auditável.

### Critérios de conclusão
- `fact_stage_progression` cobre as duas temporadas-prova de `libertadores` definidas no Bloco 0.
- Time eliminado em uma fase não aparece como participante na fase seguinte.
- Time classificado aparece exatamente uma vez como participante na fase de destino.
- A progressão e a composição dos confrontos batem nas temporadas-prova selecionadas.
- Nenhuma regressão em ligas.

### Validações obrigatórias
- Time eliminado não aparece na fase seguinte.
- Time classificado aparece exatamente uma vez na fase esperada.
- Contagem de classificados por fase confere com a configuração de progressão aprovada para a edição.
- Ligas continuam verdes.

---

## BLOCO 4 — `champions_league`: Versionamento por Temporada

**Classificação de prioridade:** piloto central obrigatório.

> **Este bloco prova que o mesmo `competition_key` pode ter formatos radicalmente diferentes por temporada, sem regressão histórica.**

### Objetivo
Materializar corretamente temporadas antigas (fase de grupos + mata-mata) e temporadas novas (league phase + mata-mata) sob o mesmo `competition_key`, garantindo que a lógica é inteiramente dirigida por `season_format_code` e `stage_format`, nunca pelo nome da competição.

### Escopo exato
Verificação e, se necessário, correção do backfill de `season_format_code` para todas as temporadas de `champions_league` no acervo.  
Implementação das duas famílias de materialização: para `ucl_group_knockout_v1`, o primeiro stage deve gerar grupos e `fact_group_standings`; para `ucl_league_table_knockout_v1`, o primeiro stage deve gerar tabela única sem grupos.  
Validação explícita de que a temporada de corte materializa corretamente para ambos os lados.  
Fases eliminatórias de ambas as famílias devem usar o mesmo modelo de `tie` do Bloco 2 — sem duplicação de lógica.  
Não existe branch `if competition == 'champions_league'` em nenhuma camada após este bloco.

### Sub-bloco 4A — Temporadas com formato de grupos
- Backfill de `season_format_code=ucl_group_knockout_v1` para temporadas aplicáveis.
- Validação: `2023/24` materializa vários grupos e nenhum stage inicial de tabela única.
- Grupos e confrontos eliminatórios funcionam via modelo existente dos Blocos 2 e 3.

### Sub-bloco 4B — Temporadas com league phase
- Backfill de `season_format_code=ucl_league_table_knockout_v1` para temporadas aplicáveis.
- Validação: `2024/25` materializa um stage inicial de tabela única e nenhum `group_id` nesse stage.
- O primeiro stage dessas temporadas usa `stage_format=league_table` e não gera grupos fictícios.

### Entregáveis
- Matriz de temporadas de `champions_league` com `season_format_code` correto, cobrindo todo o acervo.
- Materialização que seleciona a lógica correta a partir de `season_format_code` — sem nenhum branch por nome de competição.
- Validações documentadas e passando para as temporadas de corte das duas famílias.
- Confirmação de que os stages eliminatórios de ambas as famílias usam o mesmo contrato de `tie`.

### Dependências
- Blocos 2 e 3B concluídos (modelos de `tie` e de grupo validados e reutilizáveis).
- Bloco 1A concluído (tabela `competition_season_config` com `champions_league` carregada).

### Riscos
- **Alto:** temporadas antigas não exporem estrutura suficiente para grupo ou confronto no mesmo nível de granularidade. Mitigação: manter rastreabilidade e documentar cobertura real sem falsear estrutura.
- **Médio:** Temporada de corte ambígua (ex: season que estava em transição de formato). Mitigação: definir na matriz do Bloco 0 e não deixar para descobrir na implementação.

### Critérios de conclusão
- `2023/24`: vários grupos materializados, nenhum stage de tabela única.
- `2024/25`: tabela única materializada, nenhum `group_id` no primeiro stage.
- Stages eliminatórios das duas temporadas produzem o mesmo tipo de output de confronto.
- Nenhum branch de código referencia `competition_key=champions_league` como condição lógica.

### Validações obrigatórias
- Temporadas antigas e novas materializam estruturas diferentes sob o mesmo `competition_key`.
- O primeiro stage de `2024/25` não gera grupos fictícios.
- Os stages eliminatórios de ambas as famílias convergem para o mesmo contrato de confronto.
- Ligas continuam verdes.

---

## BLOCO 5A — Contratos de API e BFF

**Classificação de prioridade:** fechamento ponta a ponta dos pilotos centrais.

### Objetivo
Formalizar e implementar os contratos de consumo necessários para servir estrutura de copa, grupos e confrontos, sem quebrar os contratos existentes de liga.

### Escopo exato
Quatro contratos mínimos de consumo (novos ou como expansão compatível de contratos existentes):

**Competition structure:** dado um `competition_key` e `season_label`, retornar a sequência de fases, o formato de cada fase e a regra de progressão. Este endpoint é o hub estrutural da competição.

**Group standings:** dado `competition_key + season_label + stage_id + group_id`, retornar a tabela de classificação do grupo. O filtro por `groupId` é obrigatório para fases de grupo — não existe retorno de grupos sem scoping.

**Ties by stage:** dado `competition_key + season_label + stage_id`, retornar todos os confrontos da fase com seus resultados agregados e vencedores. Para fase eliminatória apenas.

**Team progression:** dado `competition_key + season_label + team_id`, retornar a trajetória do time na competição: quais fases disputou, resultado em cada uma.

Expansão de contratos existentes de standings: o campo `groupId` deve ser aceito como filtro opcional. Quando ausente e o stage for de grupo, o comportamento precisa ser explícito e compatível com o contrato aprovado.

### Entregáveis
- Especificação formal dos quatro contratos mínimos de consumo e das regras de compatibilidade retroativa.
- Implementação dos contratos conectando nos marts dos Blocos 2, 3 e 4.
- Versão ou backward compatibility dos contratos existentes de liga — nada que o frontend atual consome pode quebrar.
- Documentação de comportamento quando `groupId` não é informado para uma fase de grupos.

### Dependências
- Blocos 2, 3A, 3B e 4 concluídos (os dados que a API serve precisam existir).

### Riscos
- **Médio:** Contrato de standings existente não aceita `groupId` e precisa de versão. Mitigação: adicionar o campo como opcional e manter backward compatibility.
- **Baixo:** Endpoint de competition structure pode ficar pesado para competições com muitas fases. Mitigação: cachear por `competition + season`.

### Critérios de conclusão
- Os quatro contratos novos estão implementados e retornam dados corretos para os pilotos.
- Contratos existentes de liga continuam funcionando sem alteração de comportamento.
- Compatibilidade retroativa dos contratos já consumidos por liga está explicitamente provada.
- Documentação de contrato está disponível e revisada.

### Validações obrigatórias
- Endpoint de competition structure retorna fases em ordem correta com `stage_format` correto para `libertadores` e `champions_league`.
- Endpoint de group standings com filtro de grupo incorreto retorna erro claro, não dado silenciosamente errado.
- Endpoints de liga existentes continuam retornando os mesmos resultados após o deploy.

---

## BLOCO 5B — Impactos de Frontend e Produto

**Classificação de prioridade:** fechamento ponta a ponta dos pilotos centrais.

### Objetivo
Adaptar a interface do produto para reagir ao `stage_format` e ao `season_format_code`, exibindo fases de grupos, confrontos e bracket adequadamente, sem alterar o fluxo atual de ligas.

### Escopo exato
Ligas: fluxo atual sem alteração.  
Copas com `format_family=knockout` puro: exibir hub da competição com fases e confrontos eliminatórios.  
Copas com fase de `stage_format=group_table`: exibir grupos e tabelas por grupo.  
Copas com fase de `stage_format=knockout`: exibir confrontos e bracket.  
`champions_league`: exibir metadado de formato por temporada (ex: badge `2023/24 · fase de grupos + mata-mata` vs `2024/25 · league phase + mata-mata`).  
Configuração de apresentação/navegação deve permanecer separada da regra esportiva.  
Nenhum componente de frontend pode usar `competition_key` como condição de renderização. A condição deve vir da estrutura da edição e dos tipos de fase.

### Entregáveis
- Componente de hub de competição consumindo o endpoint de competition structure.
- Componente de tabela de grupo consumindo group standings com scoping por `groupId`.
- Componente de confronto eliminatório consumindo ties by stage.
- Metadado de formato de temporada exibido no catálogo da `champions_league`.
- Configuração de produto explicitamente separada de configuração de edição, fase e progressão.
- Fluxo de ligas intacto e sem regressão visual.

### Dependências
- Bloco 5A concluído (os contratos de API precisam existir antes de o frontend consumir).

### Riscos
- **Médio:** Componentes existentes de standings assumem que há apenas uma tabela por competição-temporada-fase. Mitigação: refatorar para aceitar `groupId` como parâmetro, mas manter comportamento padrão sem o parâmetro.

### Critérios de conclusão
- Fluxo de copa ponta a ponta funciona para `copa_do_brasil`, `libertadores` e `champions_league` nas temporadas piloto.
- Fluxo de liga continua sem regressão.
- Nenhum componente referencia `competition_key` como condição de renderização.

### Validações obrigatórias
- Regressão visual zero para ligas.
- Copa do Brasil exibe confrontos eliminatórios corretamente.
- Libertadores exibe grupos separados e transição para eliminatórias.
- Champions League exibe formato correto conforme a temporada selecionada.

---

## BLOCO 6 — Onboarding FIFA

**Classificação de prioridade:** expansão posterior para evitar retrabalho futuro. Não define sucesso da frente principal.

### Objetivo
Incorporar as três competições FIFA ao portfólio usando o modelo já validado, na ordem de menor para maior risco de ambiguidade semântica, somente depois de os três pilotos centrais estarem fechados ponta a ponta.

### Ordem obrigatória dentro do bloco
1. `fifa_world_cup` — prova `participant_scope=national_team`; formato mais estável histórica e estruturalmente.
2. `fifa_club_world_cup` — novo produto FIFA quadrienal; iniciar em 2025; não herdar dados de torneios anteriores.
3. `fifa_intercontinental_cup` — knockout puro anual; iniciar em 2024 como identidade própria.

### Sub-bloco 6A — `fifa_world_cup`
- Configuração de `competition_season_config` com `participant_scope=national_team` para edições disponíveis.
- Edições 2010–2022: `season_format_code=fwc_32_group_knockout_v1`.
- Edição 2026+: `season_format_code=fwc_48_group_knockout_v2`, com marcação explícita de regulamentação ainda pendente nos pontos que não estiverem fechados.
- Não tratar a seleção dos melhores terceiros do formato 2026+ como regra resolvida antes de haver definição regulamentar suficiente.
- Validação de que nenhum time de liga ou clube aparece nos dados de Copa do Mundo.

### Sub-bloco 6B — `fifa_club_world_cup`
- Iniciar em `2025` como identidade canônica própria.
- Nenhum dado do antigo FIFA Club World Cup (2000, 2005–2023) deve ser ingerido nessa identidade.
- Formato de grupos de 2025: `season_format_code=fcwc_32_group_knockout_v1`.
- Reconfirmar regulamento antes de ativar cada nova edição futura.

### Sub-bloco 6C — `fifa_intercontinental_cup`
- Iniciar em `2024` como identidade canônica própria.
- Formato knockout: `season_format_code=fic_annual_champions_knockout_v1`.
- Entrada escalonada de campeões continentais deve ser modelada como stages com `stage_format=qualification_knockout`.
- UEFA entra diretamente na final — isso é uma regra de progressão e não deve ser absorvida por `competition_season_config`.
- Antiga Intercontinental Cup (1960–2004) fica fora deste contrato.

### Entregáveis
- Três competições FIFA com dados carregados e validados nas temporadas especificadas.
- `participant_scope` correto em todas as entradas.
- Lineagens históricas excluídas e documentadas como tal.
- Regulamento pendente marcado explicitamente, sem lógica definitiva codificada para esses casos.

### Dependências
- Blocos 2, 3B e 4 concluídos (todos os modelos de suporte reutilizados aqui).
- Bloco 0 com a matriz FIFA aprovada.

### Riscos
- **Médio:** expansões FIFA inflarem a fundação antes de os três casos essenciais estarem estabilizados. Mitigação: manter este bloco como expansão posterior.
- **Médio:** mistura indevida de identidades históricas. Mitigação: manter boundaries de catálogo explícitos.

### Critérios de conclusão
- As três competições estão carregadas e consultáveis.
- `participant_scope` correto em todas as entradas.
- Nenhum dado de lineagem histórica excluída foi ingerido nas identidades novas.
- Modelos existentes de grupo, tie e progressão são reutilizados sem duplicação.

### Validações obrigatórias
- `fifa_world_cup` não tem nenhum time de clube — apenas seleções nacionais.
- `fifa_club_world_cup` não tem nenhum registro com `season_label < 2025`.
- `fifa_intercontinental_cup` não tem nenhum registro com `season_label < 2024`.
- Ligas continuam verdes.

---

## BLOCO 7 — Analytics Avançados e Produto

**Classificação de prioridade:** escopo posterior. Não entra na definição de sucesso desta frente.

### Objetivo
Construir as camadas analíticas e os componentes de produto que dependem de toda a fundação anterior estar estável.

### Escopo exato
Este bloco só começa quando os Blocos 5A, 5B e 6 estiverem concluídos e validados.

- Bracket visual completo para competições eliminatórias.
- Trajetória por time: histórico de todas as fases disputadas, resultados e classificações em cada temporada de uma competição.
- Filtros analíticos por fase: métricas de desempenho segmentadas por `stage_format`.
- Refinamentos de jogador e time por stage: artilharia por fase, aproveitamento por fase, etc.
- Comparativos históricos entre temporadas de formato diferente (ex: Champions League antes e depois da mudança).

### Dependências
- Todos os blocos anteriores concluídos.

### Critérios de conclusão
- Analytics e visualizações consomem dados via APIs estabelecidas — sem queries diretas ad hoc em produção.
- Toda métrica tem denominador correto (ex: média por jogo em fase de grupos vs média por confronto em eliminatória).

---

## BLOCO 8 — Casos Complexos e Edge Cases

**Classificação de prioridade:** escopo posterior. Não entra na definição de sucesso desta frente.

### Objetivo
Tratar casos excepcionais que deliberadamente foram deixados fora do escopo dos blocos de piloto para não aumentar risco e complexidade na fundação.

### Escopo exato — somente após todos os blocos anteriores estarem verdes
- Repescagem entre competições (ex: time que cai da Champions vai para a Europa League).
- Regras administrativas retroativas (ex: desclassificação após o torneio).
- Critérios de desempate multi-nível raros (ex: confronto direto entre três times empatados).
- Formatos exóticos futuros não previstos na matriz atual.
- Migração de nomenclatura `league_*` → `competition_*` — apenas aqui, depois de tudo estável.

### Dependências
- Todos os blocos anteriores concluídos e validados.

### Critérios de conclusão
- Cada edge case é tratado via configuração na boundary correta ou extensão controlada do modelo — nunca via hardcode e nunca por sobrecarga indevida de `competition_season_config`.

---

## ORDEM RECOMENDADA DE IMPLEMENTAÇÃO REAL

```
Fase 0 (Pré-requisito absoluto)
  └── Bloco 0 — Contrato semântico e matriz de regras

Fase 1 (Fundação — nenhum piloto começa sem isso)
  ├── Bloco 1A — competition_season_config
  ├── Bloco 1B — dim_stage ampliado
  └── Bloco 1C — fact_matches e standings extensão aditiva

Fase 2 (Piloto 1 — knockout puro)
  └── Bloco 2 — copa_do_brasil

Fase 3 (Piloto 2 — grupos e híbrido)
  ├── Bloco 3A — libertadores fase de grupos
  └── Bloco 3B — libertadores ponta a ponta

Fase 4 (Piloto 3 — versionamento por temporada)
  └── Bloco 4 — champions_league (sub-blocos 4A e 4B em sequência)

Fase 5 (Superfície — API e produto)
  ├── Bloco 5A — contratos de API e BFF
  └── Bloco 5B — frontend e produto

Fase 6 (Expansão posterior — FIFA; não bloqueia sucesso da frente principal)
  ├── Bloco 6A — fifa_world_cup
  ├── Bloco 6B — fifa_club_world_cup
  └── Bloco 6C — fifa_intercontinental_cup

Fase 7 (Posterior à estabilização dos pilotos centrais)
  ├── Bloco 7 — analytics avançados e produto
  └── Bloco 8 — casos complexos e edge cases
```

**Blocos 1A, 1B e 1C podem ser desenvolvidos em paralelo entre si, mas nenhum pode ser pulado.**  
**Blocos 3A e 3B são sequenciais entre si; 3A não depende de 2 ser concluído, mas 3B depende de 2.**  
**Blocos 5A e 5B são sequenciais entre si.**  
**Blocos 6A, 6B e 6C são sequenciais entre si na ordem indicada.**  
**Blocos 5A e 5B não começam com contrato hipotético: só começam após os pilotos centrais terem evidência de dados validada.**  
**Concluir Fases 0 a 5 define o sucesso real desta frente. Fases 6 a 8 só entram depois, sem disputar prioridade.**

---

## CRITÉRIOS DE GO / NO-GO POR FASE

### Fase 0 → GO para Fase 1
- [ ] Contratos semânticos mínimos do modelo aprovados por alguém com domínio esportivo.
- [ ] Boundaries entre configuração de edição, fase, progressão, produto e exceções aprovados.
- [ ] Todos os pilotos têm `season_format_code` definido.
- [ ] `copa_do_brasil` e `libertadores` têm temporadas-prova fixadas a partir do acervo atual.
- [ ] Casos de inferência identificados e documentados.
- [ ] Champions League tem mapeamento por temporada completo.

### Fase 1 → GO para Fase 2
- [ ] `competition_season_config` criada, carregada e com testes passando.
- [ ] `dim_stage` ampliada com campos nullable — testes existentes passando.
- [ ] `fact_matches` e `fact_standings_snapshots` com campos novos nullable — grain de liga inalterado.
- [ ] `competition_season_config` continua restrita ao escopo macro da edição.
- [ ] `dim_stage` não absorveu progressão, produto nem exceções regulamentares.
- [ ] Rerun idempotente de todas as ligas retorna resultados idênticos ao pré-migração.

### Fase 2 → GO para Fase 3
- [ ] `fact_tie_results` validado para as duas temporadas-prova de `copa_do_brasil`.
- [ ] Todo confronto completo tem exatamente um vencedor.
- [ ] Gols agregados batem com `fact_matches`.
- [ ] Progressão básica da `copa_do_brasil` fecha nessas mesmas temporadas-prova.
- [ ] Inferências marcadas corretamente.
- [ ] Zero regressão em ligas.

### Fase 3 → GO para Fase 4
- [ ] `fact_group_standings` validado para as temporadas-prova de `libertadores`.
- [ ] `fact_group_standings` com grain único por `competition-season-stage-group-round-team`.
- [ ] Zero mistura de grupos no mesmo snapshot.
- [ ] `fact_stage_progression` cobrindo as temporadas-prova de `libertadores` com classificados corretos.
- [ ] Time eliminado não aparece em fase seguinte.
- [ ] Zero regressão em ligas.

### Fase 4 → GO para Fase 5
- [ ] `champions_league 2023/24` materializa grupos; nenhum stage de tabela única.
- [ ] `champions_league 2024/25` materializa tabela única; nenhum grupo fictício.
- [ ] Nenhum branch de código usa `competition_key` como condição lógica.
- [ ] Zero regressão em ligas.

### Fase 5 → GO para Fase 6 (somente se a expansão FIFA for iniciada)
- [ ] Estrutura mínima consumível da edição está exposta pelos contratos aprovados.
- [ ] Quatro contratos mínimos de consumo implementados e documentados.
- [ ] Contratos de liga existentes inalterados.
- [ ] Compatibilidade retroativa dos fluxos de liga provada em API/BFF e produto.
- [ ] Frontend exibe copa ponta a ponta para os três pilotos.
- [ ] Zero regressão visual em ligas.

### Fase 6 → GO para Fase 7 (somente após a expansão FIFA)
- [ ] FIFA World Cup sem times de clube.
- [ ] FIFA Club World Cup sem dados anteriores a 2025.
- [ ] FIFA Intercontinental Cup sem dados anteriores a 2024.
- [ ] Modelos de grupo, tie e progressão reutilizados sem duplicação.
- [ ] Zero regressão em ligas e pilotos anteriores.

### Fase 7 e 8
- [ ] Só iniciar após todas as fases anteriores aprovadas.
- [ ] Qualquer edge case tratado via configuração — zero hardcode.
- [ ] Migração de nomenclatura `league_*` → `competition_*`, se feita, é a última coisa — depois de tudo estável.

---

## CONCLUSÃO — ARQUITETURA MÍNIMA RECOMENDADA PARA COMEÇAR SEM RETRABALHO

A arquitetura mínima recomendada para iniciar esta evolução sem retrabalho é:

- manter `competition` como identidade estável de catálogo e `competition season` como edição versionada, sem sobrecarregar a edição com semântica de fase, progressão ou produto;
- introduzir `stage` como unidade estrutural da edição, com `group`, `tie` e `leg` como entidades próprias quando existirem de fato no torneio;
- tratar `progression` como contrato independente entre origem e destino, nunca como atributo implícito de `stage`;
- expor `standings context` e `bracket context` como contextos semânticos distintos e consumíveis pelo produto;
- separar, desde o início, configuração de edição, configuração de fase, configuração de progressão, configuração de apresentação e exceções regulamentares;
- preservar compatibilidade com ligas por expansão aditiva, sem rename amplo e sem branch por nome de competição.

Com essa fundação, `copa_do_brasil`, `libertadores` e `champions_league` passam a caber no mesmo modelo sem reduzir copa a liga adaptada, sem reduzir produto a uma única navegação e sem abrir dívida estrutural desnecessária para as expansões FIFA posteriores.
