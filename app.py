CSS = """
<style>
:root{
  --bg:#07080b;
  --card:#0f1116;
  --card2:#0b0d12;
  --text:#e5e7eb;
  --muted:#9ca3af;

  /* 冷酷黑灰主調 */
  --accent:#e5e7eb;
  --accent2:#6b7280;
  --ok:#a3e635;
  --warn:#cbd5e1;
  --bad:#fb7185;

  --line:rgba(148,163,184,.14);
}

/* 全局背景 */
.main { background: var(--bg); }
.block-container { padding-top: 1.2rem; padding-bottom: 2.2rem; }

/* Header */
.header-wrap{
  display:flex; align-items:flex-end; justify-content:space-between;
  gap:18px; padding: 10px 4px 6px 4px;
}
.title{
  font-size: 44px; font-weight: 900; letter-spacing: .5px;
  background: linear-gradient(90deg, #f3f4f6, #9ca3af);
  -webkit-background-clip:text; -webkit-text-fill-color: transparent;
  margin:0;
}
.subtitle{
  margin:6px 0 0 2px; color: var(--muted); font-size: 14px;
}

/* 右上角 pill */
.pill{
  display:inline-flex; align-items:center; gap:8px;
  padding: 8px 12px; border:1px solid var(--line);
  border-radius: 999px; color: var(--text);
  background: rgba(15,17,22,.72);
  font-size: 13px;
  box-shadow: 0 10px 30px rgba(0,0,0,.25);
}
.pill b{ color: var(--text); }
.pill .dot{ width:8px; height:8px; border-radius:999px; background:#9ca3af; display:inline-block; }

/* 卡片 grid */
.grid{
  display:grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin: 14px 0 6px 0;
}
.card{
  background: linear-gradient(180deg, rgba(15,17,22,.92), rgba(11,13,18,.92));
  border:1px solid var(--line);
  border-radius: 16px;
  padding: 14px 14px 12px 14px;
  box-shadow: 0 16px 40px rgba(0,0,0,.35);
}
.k{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }
.v{ color: var(--text); font-size: 20px; font-weight: 800; }
.v small{ color: var(--muted); font-weight: 600; font-size: 12px; margin-left: 6px;}

/* 分隔線 */
.hr{ height:1px; background: var(--line); margin: 12px 0; }

/* 橫幅提示 */
.banner{
  background: rgba(148,163,184,.08);
  border: 1px solid rgba(148,163,184,.22);
  color: var(--text);
  border-radius: 16px;
  padding: 12px 14px;
  margin: 10px 0 10px 0;
}
.banner b{ color: #fff; }

/* TOP 卡片資訊 */
.metric{
  display:flex; justify-content:space-between; align-items:flex-end;
  gap:10px;
}
.metric .left{ display:flex; flex-direction:column; gap:2px; }
.metric .label{ color: var(--muted); font-size: 12px; }
.metric .code{ color: var(--text); font-size: 16px; font-weight: 800; }
.metric .tag{
  font-size: 12px; padding: 4px 8px; border-radius: 999px;
  border:1px solid var(--line); color: var(--text);
  background: rgba(15,17,22,.7);
}
.metric .price{ font-size: 22px; font-weight: 900; color: var(--text); line-height: 1; }
.metric .chg{ font-size: 12px; color: var(--muted); }

/* 按鈕：冷酷黑灰 */
.stButton>button{
  border-radius: 14px !important;
  border: 1px solid rgba(203,213,225,.28) !important;
  background: linear-gradient(90deg, rgba(148,163,184,.18), rgba(107,114,128,.12)) !important;
  color: var(--text) !important;
  font-weight: 800 !important;
  padding: 10px 14px !important;
}
.stButton>button:hover{
  border: 1px solid rgba(203,213,225,.45) !important;
  background: linear-gradient(90deg, rgba(148,163,184,.24), rgba(107,114,128,.16)) !important;
}

/* 下拉與輸入框更貼黑灰 */
.stSelectbox>div>div,
.stTextInput>div>div,
.stNumberInput>div>div{
  border-radius: 14px !important;
  border: 1px solid rgba(148,163,184,.20) !important;
  background: rgba(15,17,22,.72) !important;
  color: var(--text) !important;
}

/* 小字 */
.small-note{ color: var(--muted); font-size: 12px; }

/* 表格（dataframe）背景更黑 */
[data-testid="stDataFrame"]{
  border-radius: 16px;
  border: 1px solid var(--line);
  overflow: hidden;
}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)
