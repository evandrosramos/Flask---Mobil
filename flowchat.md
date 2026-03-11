flowchart TD
%% Cores e Estilos para o LinkedIn
classDef hardware fill:#e2e8f0,stroke:#2b6cb0,stroke-width:2px,color:#2d3748,font-weight:bold
classDef python fill:#c6f6d5,stroke:#276749,stroke-width:2px,color:#276749,font-weight:bold
classDef db fill:#fefcbf,stroke:#b7791f,stroke-width:2px,color:#744210,font-weight:bold
classDef web fill:#bee3f8,stroke:#2b6cb0,stroke-width:2px,color:#2a4365,font-weight:bold

    %% Nós do Fluxo
    CLP["🏭 Chão de Fábrica   (CLP Allen-Bradley)"]
    COLETOR["⚙️ Motor de Coleta (Python + pylogix)"]
    BANCO[("🗄️ Banco de Dados (PostgreSQL)")]
    BACKEND["🌐 Servidor Web     (Flask REST API)"]
    DASHBOARD["📊 Dashboard     (HTML/JS + Chart.js)"]

    %% Aplicação das classes (estilos)
    class CLP hardware;
    class COLETOR,BACKEND python;
    class BANCO db;
    class DASHBOARD web;

    %% Conexões com rótulos explicativos
    CLP -->|"Leitura de Tags via EtherNet/IP"| COLETOR
    COLETOR -->|"SQL Alchemy Gravação Contínua"| BANCO
    BANCO -->|"Consultas Otimizadas"| BACKEND
    BACKEND -->|"APIs JSON Tempo Real"| DASHBOARD
