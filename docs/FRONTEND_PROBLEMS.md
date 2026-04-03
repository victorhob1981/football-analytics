# Football Analytics — análise inicial de problemas observados no frontend

> **Status deste documento:** análise inicial baseada em inspeção manual de telas e navegação.
>
> Este material **não deve ser tratado como diagnóstico final**. Cada item abaixo **precisa ser investigado a fundo** no código, nos contratos do BFF, na modelagem de dados e no comportamento real da interface antes de qualquer correção.

## Objetivo deste documento

Consolidar, de forma mais clara e consistente, os problemas observados durante uma rodada manual de uso do produto, agrupando:
- falhas funcionais
- problemas de conteúdo e UX
- poluição visual
- inconsistências de layout
- possíveis gaps de dados/contrato
- padrões repetidos entre telas

Também foram incluídas **hipóteses iniciais** e **direções de solução**, sempre como ponto de partida para investigação.

---

## 1. Problemas transversais do produto

### 1.1 Excesso de linguagem técnica/interna exposta ao usuário
Há um padrão recorrente de textos que parecem escritos para time interno, validação técnica ou documentação de implementação, e não para usuário final.

Exemplos recorrentes:
- “contexto canônico”
- “shell”
- “hub”
- “núcleo”
- “coverage”
- “entrypoint”
- “resolver curto”
- “registry atual”
- “sem app paralelo”
- “telas que já estão verdes”
- “cards extras continuam fora deste ciclo”
- referências explícitas a endpoints como `GET /api/...`

#### Impacto
- o produto parece inacabado ou interno
- a interface “fala da arquitetura” em vez de mostrar valor
- o usuário precisa ler muito texto sem benefício real
- a percepção de qualidade cai, mesmo quando a funcionalidade existe

#### Direção inicial
- revisar toda a camada textual com foco em linguagem de produto
- remover ou substituir textos de implementação por conteúdo útil ao usuário
- migrar explicações técnicas para documentação interna, não para a UI

---

### 1.2 Poluição visual e excesso de texto
Em várias telas, existe excesso de blocos, rótulos, banners, chips e explicações curtas acumuladas em áreas nobres da interface.

Padrões percebidos:
- muito texto para pouco valor informacional
- repetição de conceitos no mesmo trecho da tela
- áreas visuais ocupadas por metadados do sistema
- alertas e banners que chamam atenção sem necessidade real
- espaços grandes com pouco conteúdo útil

#### Impacto
- piora da hierarquia visual
- sensação de produto inflado e pouco objetivo
- fadiga de leitura
- menor clareza sobre o que realmente importa em cada página

#### Direção inicial
- fazer revisão transversal de conteúdo e densidade visual
- reduzir textos que não geram ação, descoberta ou valor
- revisar banners, labels e blocos auxiliares
- priorizar conteúdo informacional real e navegação clara

---

### 1.3 Interface com cara de ferramenta interna, não de produto final
Há um padrão consistente de telas que parecem desenhadas para provar que a arquitetura existe, em vez de atender alguém que quer usar o sistema.

#### Sinais disso
- metadados de sistema aparecendo como destaque
- explicações sobre como o produto foi construído
- textos sobre comportamento interno da navegação
- cards mostrando estrutura do app em vez de conteúdo do futebol

#### Impacto
- percepção de maturidade do produto cai
- a aplicação parece “validando conceitos” e não entregando valor
- a interface transmite mais engenharia do que utilidade

#### Direção inicial
- reposicionar o produto como experiência de exploração de dados
- trocar metainformação por informação útil
- priorizar “o que o usuário quer ver/fazer” em vez de “como a tela funciona por baixo”

---

### 1.4 Hierarquia visual destacando as coisas erradas
Em várias telas, elementos com pouco valor ficam visualmente mais fortes do que o conteúdo realmente importante.

Exemplos de destaque errado:
- chips e labels decorativos
- banners de cobertura
- descrições de arquitetura
- cards com números internos
- caixas explicativas de “navegação e contexto”

#### Impacto
- o usuário não entende rapidamente a proposta da tela
- ações e informações relevantes perdem destaque
- o design parece desbalanceado

#### Direção inicial
- revisar hierarquia visual por prioridade de uso
- reforçar o que ajuda descoberta, leitura e navegação
- reduzir o peso de metadados, alertas supérfluos e conteúdo operacional

---

## 2. Problemas de dados e cobertura

### 2.1 Nacionalidade ausente para jogadores
Na lista de jogadores, todos os registros observados aparecem com **“Nacionalidade não informada”**.

#### Hipótese inicial
- a fonte/origem pode fornecer esse dado
- o banco pode não estar persistindo isso
- o BFF pode não estar expondo esse campo
- o frontend pode não estar usando o contrato correto

#### Impacto
- empobrece bastante a experiência
- passa sensação de dado incompleto ou mal estruturado
- reduz valor dos cards/listas de jogadores

#### Direção inicial
- verificar se o provider realmente entrega nacionalidade
- mapear se o dado existe no raw/warehouse/mart
- validar persistência, transformação, contrato e renderização
- caso a cobertura seja parcial, tratar ausência com mais critério

---

## 3. Problemas na busca global

### 3.1 Busca não encontrou “Flamengo”
Ao buscar **Flamengo**, o sistema retornou **sem resultados**.

#### Hipóteses iniciais
- problema de coverage da busca
- problema de indexação ou matching
- restrição excessiva ao conjunto “navegável”
- ausência de contexto canônico resolvido para esse time
- falha na camada de search/BFF para entidades relevantes

#### Impacto
- quebra forte de confiança na busca
- afeta percepção do produto como um todo
- um termo óbvio e importante falhar é grave para portfolio/demo

#### Direção inicial
- investigar presença do Flamengo nas entidades indexadas
- validar a lógica de busca e o recorte de resultados retornáveis
- revisar matching por nome popular, nome oficial, aliases e contexto navegável

---

### 3.2 Overlay de busca com linguagem técnica demais
O overlay exibe mensagens como:
- “resultados só usam rotas e contextos já suportados”
- “global search navigability coverage (100.0%)”

#### Impacto
- linguagem interna/técnica exposta
- poluição visual
- aparência de ambiente de QA/validação

#### Direção inicial
- remover indicadores operacionais da interface final
- manter mensagens curtas e orientadas a uso
- expor informações técnicas apenas em ambiente interno, se realmente necessário

---

## 4. Problemas na home executiva e entrada do produto

### 4.1 Home com conteúdo fraco e pouco relevante
A crítica principal não foi ao design em si, mas ao **conteúdo**.

#### Problemas percebidos
- muita mensagem abstrata
- pouca informação relevante logo na abertura
- cards com utilidade fraca
- home sem “gancho” informacional forte
- sensação de conteúdo filler

#### Impacto
- a página inicial não ajuda o usuário a entender valor
- a abertura do produto não é memorável nem útil
- a home parece explicar o app em vez de entregar algo interessante

#### Direção inicial
- repensar a home com foco em informação útil já na primeira dobra
- substituir explicações abstratas por conteúdo acionável
- usar a home para dar contexto, descoberta e valor, não só framing

---

### 4.2 Topo/home com duplicação e excesso de contexto
O topo da home foi percebido como duplicado, poluído e visualmente inconsistente.

#### Problemas percebidos
- sensação de repetição entre blocos
- muita informação concentrada em pouco espaço
- duplicação conceitual
- ruído visual logo na entrada do produto

#### Direção inicial
- simplificar a camada superior da home
- evitar duas seções competindo pela mesma função
- reduzir redundância de rótulos, filtros e mensagens

---

## 5. Problemas em competições, competition hub e season hub

### 5.1 Competition hub com texto arquitetural demais
Na tela de competição, o conteúdo fala mais da estrutura do app do que do campeonato.

#### Problemas percebidos
- texto técnico/interno demais
- hero grande com pouco valor
- cards laterais com dados pouco relevantes ao usuário
- bloco “Escopo deste núcleo” inadequado para produto final
- labels/chips pouco úteis ou ambíguos

#### Impacto
- a tela parece “hub técnico”, não exploração de campeonato
- muito espaço nobre é gasto com explicação interna
- o campeonato em si perde protagonismo

#### Direção inicial
- remover ou substituir blocos internos
- usar o espaço para contexto útil do campeonato
- enriquecer a entrada com informação de produto real

---

### 5.2 Season hub com muito texto inútil e pouco valor
A tela segue o mesmo padrão de explicar arquitetura e navegação, com pouco conteúdo útil ao usuário.

#### Problemas percebidos
- muito texto interno
- pouco valor informacional
- cards repetitivos
- links rápidos com linguagem de implementação
- baixo aproveitamento do espaço

#### Direção inicial
- cortar textos internos
- simplificar a seção
- transformar a página em ponto de entrada útil para a temporada, não em descrição do framework de navegação

---

### 5.3 Tela de competições com excesso de texto e affordance ruim
A listagem de competições também foi percebida como excessivamente explicativa e pouco orientada a valor.

#### Problemas percebidos
- excesso de explicação interna
- pouca informação realmente útil sobre cada competição
- existência de área branca clicável sem indicação clara
- affordance fraca: ação descoberta “por sorte”

#### Direção inicial
- reduzir texto de arquitetura
- reforçar sinalização de elementos clicáveis
- revisar card/lista de competição com foco em valor e clareza

---

## 6. Problemas na navegação superior

### 6.1 Barra do topo quebrando em duas linhas
A navegação principal ficou com 4 itens em uma linha e 2 em outra.

#### Impacto
- piora a leitura
- deixa a barra menos limpa
- transmite sensação de desorganização

#### Direção inicial
- revisar a distribuição horizontal do header
- reposicionar marca/título
- ajustar largura e comportamento responsivo para manter os 6 itens em uma linha, se isso for viável e consistente

---

## 7. Problemas em rankings

### 7.1 Filtro/busca do ranking causa refresh e reposicionamento da página
Ao digitar qualquer letra, a página atualiza e sobe, obrigando o usuário a descer novamente.

#### Impacto
- quebra severa de usabilidade
- filtro fica irritante de usar
- experiência de exploração é prejudicada

#### Direção inicial
- investigar se o input aciona navegação/refresh imediato
- revisar sincronização com rota/query string
- aplicar atualização menos agressiva ou debounce adequado

---

### 7.2 Cards dos jogadores visualmente quebrados
Os cards superiores do ranking têm:
- quebra ruim do nome
- número mal encaixado
- muito espaço vazio na base
- composição fraca

#### Preferência observada
Foi indicada preferência por **enriquecer o card com informação útil**, em vez de apenas encolher/remover espaço.

Sugestões citadas:
- time
- nacionalidade
- detalhamento por temporada
- outros dados relevantes

#### Direção inicial
- revisar densidade e composição do card
- avaliar inclusão de dados úteis no espaço ocioso
- melhorar encaixe entre nome, valor e contexto do jogador

---

### 7.3 Banner de cobertura com aparência de alerta sem motivo real
O banner em laranja com “cobertura parcial / cobertura atual 100%” foi percebido como inútil e confuso.

#### Impacto
- ruído visual
- falsa sensação de alerta
- mistura semântica ruim entre problema e estado normal

#### Direção inicial
- remover o banner quando não houver risco/limitação relevante
- revisar a semântica de coverage na UI
- mostrar esse tipo de informação só quando for realmente necessário

---

## 8. Problemas em teams e resolvers de rota

### 8.1 Perfil do Flamengo não abre corretamente
Ao tentar abrir o perfil do Flamengo, o produto exibiu **“Contexto de time não resolvido”**.

#### Problemas percebidos
- falha funcional real em entidade importante
- rota curta/canônica não resolve
- mensagem de erro expõe implementação interna
- fluxo básico de navegação quebra

#### Hipóteses iniciais
- ausência de contexto canônico resolvível no BFF
- problema de mapeamento entre time e temporada/competição
- problema no resolver curto
- cobertura de contexto insuficiente para clubes fora do recorte mais estável

#### Direção inicial
- investigar a cadeia completa de resolução do Flamengo
- validar se existe contexto canônico correto disponível
- revisar comportamento de fallback
- trocar mensagem técnica por erro de produto mais adequado

---

## 9. Problemas em matches / match center

### 9.1 Erro visível ao abrir detalhes de uma partida
Ao abrir uma partida, a tela carregou, mas exibiu erro técnico do React/Next indicando chaves duplicadas:
- `Encountered two children with the same key, 'Rodada 38'`

#### Leitura inicial
O erro aparenta vir da renderização de tags/itens na shell/frame da página, com `key` não única.

#### Impacto
- passa sensação de produto quebrado
- compromete estabilidade percebida
- pode causar comportamento inconsistente de renderização

#### Direção inicial
- revisar geração das `scopeTags`
- garantir unicidade das chaves renderizadas
- validar se o problema é de conteúdo duplicado ou apenas de key incorreta

---

## 10. Problemas recorrentes de conteúdo e UX que devem entrar no escopo geral

### 10.1 Revisão transversal de poluição visual
O app precisa de revisão geral buscando:
- textos desnecessários
- textos exagerados
- banners sem utilidade
- rótulos repetitivos
- metainformação sem valor
- explicações técnicas expostas
- blocos que ocupam espaço sem entregar nada ao usuário

### 10.2 Revisão transversal de hierarquia e densidade informacional
O app também precisa de revisão geral buscando:
- áreas vazias demais
- cards grandes com pouco conteúdo
- elementos úteis sem destaque
- elementos secundários supervalorizados
- falta de “conteúdo forte” em áreas nobres

### 10.3 Revisão transversal de affordance e clareza interativa
Também apareceu necessidade de revisar:
- elementos clicáveis sem sinalização
- links ou áreas acionáveis descobertos por acaso
- controles que causam efeitos inesperados
- interações que reposicionam a tela ou interrompem o fluxo

---

## Resumo executivo

Os problemas observados até aqui se concentram em quatro frentes principais:

1. **Conteúdo e linguagem**
   - excesso de texto técnico/interno
   - pouco valor percebido
   - interface falando de arquitetura em vez de futebol/produto

2. **UX e clareza**
   - filtros irritantes
   - interações pouco claras
   - links clicáveis mal sinalizados
   - erros de navegação importantes

3. **Qualidade visual e densidade**
   - poluição visual
   - espaço desperdiçado
   - hierarquia ruim
   - cards/blocos mal aproveitados

4. **Dados e estabilidade**
   - nacionalidade ausente
   - busca falhando para Flamengo
   - perfil do Flamengo quebrando
   - detalhe de partida abrindo com erro de renderização

---

## Próximo uso recomendado deste documento

Este material serve como **base inicial de triagem**. O uso mais adequado dele é:

1. agrupar problemas por tema
2. validar quais são bugs funcionais reais
3. separar o que é:
   - dado
   - contrato/BFF
   - resolver/rota
   - UX/conteúdo
   - layout/visual
4. priorizar correções por impacto
5. só então desenhar o plano de execução

---

## Observação final

Nada aqui deve ser tratado como verdade final sem investigação.

Alguns itens provavelmente são:
- bug real
- falha de contrato
- falha de UX
- mau texto
- ruído visual

Outros podem ser:
- efeito colateral da modelagem atual
- recorte incompleto de cobertura
- componente provisório que foi promovido cedo demais para camada final

Por isso, o caminho correto é usar este documento como **base de diagnóstico inicial**, e não como sentença definitiva sobre a origem de cada problema.
