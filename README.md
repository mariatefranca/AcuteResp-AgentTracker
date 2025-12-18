# AcuteResp-AgentTracker: Agente Analista de Síndrome Respiratória Aguda Grave (DataSUS)

Este projeto implementa uma **solução de IA generativa com agentes** para ingestão, transformação e análise de dados de **SRAG (Síndrome Respiratória Aguda Grave)** disponibilizados pelo **DataSUS**.
O agente gera automaticamente o relatório diário de SRAG e também pode ser usado pelo usuário para obter informações sobre a doença ou os dados existentes. Para interagir com o agente utilize o notebook chat_ai.

A solução gera **relatórios HTML automatizados**, contendo:
- Principais **métricas epidemiológicas** sobre a doença
- **Visualizações interativas** (gráficos e tabelas)
- **Explicações em linguagem natural**, contextualizando os indicadores calculados com base em **notícias diárias da web**, permitindo uma leitura analítica e atualizada do cenário epidemiológico

O projeto foi desenvolvido para rodar no **Databricks Free Edition**, utilizando **Databricks Asset Bundles (DAB)** para padronizar o deploy e a execução.

O fluxo principal é composto por um **agente de IA generativa** que executa as seguintes etapas:

1. **Ingestão de dados**  
   - Download e leitura dos dados públicos de SRAG do DataSUS

2. **Transformação**  
   - Limpeza e padronização dos dados
   - Criação de métricas epidemiológicas (casos, óbitos, taxas, evolução temporal, etc.)

3. **Geração de visualizações**  
   - Gráficos temporais, distribuições geográficas e indicadores-chave

4. **Contextualização com IA generativa**  
   - Coleta de notícias recentes da web
   - Geração de explicações textuais que relacionam os dados com o contexto atual

5. **Geração de relatórios HTML**  
   - Relatórios prontos para compartilhamento

## 📁 Estrutura do Repositório

```
.   
├── conf/                               # Arquivos de configuração.   
├── reports/                            # Relatórios diários de SRAG.   
│   └── report.html                     # Relatório final em HTML gerado para o dia.  
├── scratch/                            # Notebooks de exploração.   
├── resources/                          # Notebooks para exploração.   
├── src/                                # Código-fonte principal da aplicação.   
│   ├── agents/                         # Agentes responsáveis pela execução do relatório.
│       └── agent.py                    # Código de estruturação do agente.  
│       └── agent_environment.py        # Código para criação do serving endpoint.   
│       └── deploy_agent.py             # Código para execução do deploy do agente.   
│       └── daily_report_generator.py   # Notebook que executa o modelo para gerar o relatório diário de SRAG
│       └── chat_ai.py                  # Notebook que carrega o modelo e permite o envio de perguntas personalizadas aoa gente. 
│   ├── agent_config/                   # Arquivos de configuração do agente.   
│       └── callback_handler.py         # Código para captura de logs de eventos nas chamadas do agente.   
│       └── prompt.py                   # System prompt.   
│   ├── elt/                            # Extract, load and transform data.   
│       └── extract_static_data.py      # Extração de dados antigos/estáticos.   
│       └── extract_refreshing_data.py  # Extração de dados que são atualidos com frequência.   
│       └── feature_engineering.py      # Tranformação de dados para  engenharia de features.   
│   ├── tools/                          # Ferramentas (tools) executadas pelos agentes.   
│       └── metric_calculator.py        # Ferramenta que calcula as métricas de SRAG.   
│       └── visual_generator.py         # Ferramenta que plota as visualizações do relatório.   
│       └── web_news_searcher.py        # Ferramenta que recebe uma query do agente e faz uma busca de notícias na web.   
│       └── database_searcher.py        # Agente/Ferramenta que faz spark queries nas tabelas para responder perguntas 
│                                         do usuário sobre os dados.   
│       └── report_finder.py            # Ferramenta que busca se existe um relatório já gerado no dia atual.
│       └── report_assembler.py         # Ferramenta que recebe dicionários contendo notícias, métricas e visualizações e 
│                                         compila o relatório.   
│   ├── utils/                          # Arquivos utilitários.   
│       └── srag_report_template.html   # Template do relatório.   
│       └── general_helpers.py          # Funções utilitárias.  
├── .env.example                        # Arquivo de exemplo para variáveis de ambiente.   
├── pyproject.toml                      # Arquivo de configuração de dependências Python.   
└── README.md                           # Documentação do projeto.   
```


## Pré-requisitos

- Conta **Databricks Free Edition**
- Conta no **GitHub**
- OpenAI API chave paga  GPT-5 (https://openai.com/index/openai-api/)
- Free API key Tavily search (https://www.tavily.com/)
- Git instalado localmente (opcional, mas recomendado quando usado localmente)
- Databricks CLI (uso local)


## Como criar uma conta no Databricks Free Edition

1. Acesse: https://www.databricks.com/try-databricks
2. Selecione **Free Edition**
3. Crie sua conta utilizando e-mail ou login do GitHub
4. Após a criação, você será redirecionado para o **Databricks Workspace**

A Free Edition é suficiente para executar este projeto e testar agentes de IA.


## Como configurar a conexão do GitHub com o Databricks

1. No Databricks Workspace, clique no seu avatar (canto superior direito)
2. Vá em **Settings → Linked accounts**
3. Em **Git Integration**, selecione **GitHub**
4. Autorize o acesso do Databricks à sua conta GitHub

Alternativamente, você pode usar um **GitHub Personal Access Token (PAT)**:
- Crie o token no GitHub
- Cole o token na configuração de Git Integration do Databricks


## Como clonar o repositório no Databricks


### Opção 1 – Usando a interface do Databricks

1. No Workspace, clique em **Repos**
2. Clique em **Add Repo**
3. Selecione **Clone remote Git repo**
4. Informe a URL do repositório GitHub
5. Clique em **Create Repo**

### Opção 2 – Localmente via Databricks CLI

```bash
databricks repos create https://github.com/seu-usuario/seu-repositorio
```


##  Como rodar o projeto com Databricks Asset Bundles

### Adicione API keys

Adicione as chaves da Open AI e Tavily Search no arquivo .env.example e renomeio para .env.

## Jobs a serem executados

- project_setup_job - Job que faz a extração, processamento dos dados, registra o modelo de IA generativa e chama o modelo para gerar o realtório de SRAG (roda manualmente sob demanda).
- acute_resp_agent_job - Job que tenta extrair dados novos, aplica a transfomraçào nos dados, e chama o modelo para gerar o relatório de SRAG. Job executado diarimente de forma automática após o deploy.

### Opção 1 – Usando a interface do Databricks

Para implantar e gerenciar este asset bundle, siga os passos abaixo:

1. Implantação

- Clique no **ícone de foguete de implantação** 🚀 na barra lateral esquerda para abrir o painel **Deployments** e, em seguida, clique em **Deploy**.

2. Execução de Jobs e Pipelines

- Execute o job implantado project_setup_job, clicando no botão **Run** (play) no canto direito do job.
- O job acute_resp_agent_job será executado diariamente para tentar extrair dados atualizados e gerar o relatório diário de SRAG.


### Opção 2 – Localmente via Databricks CLI

1. Instalar o Databricks CLI

```bash
pip install databricks-cli
```

2. Autenticar no Databricks

```bash
databricks auth login
```
Siga as instruções para autenticar via navegador.

A partir da raiz do repositório:

4. Validar o bundle

```bash
databricks bundle validate
```

5. Fazer o deploy do bundle

```bash
databricks bundle deploy
```

6. Executar o pipeline

```bash
databricks bundle run
```

> O comando `run` executa os jobs definidos no arquivo `databricks.yml`, incluindo a execução do agente de IA e a geração dos relatórios.

## Resultados

Ao final da execução, o projeto gera:
- Relatórios **HTML** com métricas de SRAG
- Gráficos e indicadores epidemiológicos
- Texto explicativo gerado por IA, contextualizado com notícias recentes

Os relatórios podem ser acessados diretamente no **Databricks Workspace** ou exportados para compartilhamento.