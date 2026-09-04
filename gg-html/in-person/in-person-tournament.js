(function () {
  "use strict";

  const DEFAULT_API_BASE = "https://api.carcassonne.gg";
  const TAB_DEFINITIONS = [
    { id: "playoffs", uk: "Плейоф", en: "Playoffs" },
    { id: "swiss", uk: "Швейцарка", en: "Swiss" },
    { id: "rounds", uk: "Раунди", en: "Rounds" },
    { id: "players", uk: "Гравці", en: "Players" },
  ];
  const script = document.currentScript;

  function element(tagName, className = "", text = null) {
    const node = document.createElement(tagName);
    if (className) node.className = className;
    if (text !== null && text !== undefined) node.textContent = String(text);
    return node;
  }

  function appendTextParts(container, parts, separator = " • ") {
    parts.filter((part) => part !== null && part !== undefined && String(part).trim()).forEach((part, index) => {
      if (index) container.appendChild(document.createTextNode(separator));
      container.appendChild(document.createTextNode(String(part)));
    });
  }

  function safeHttpUrl(value) {
    try {
      const parsed = new URL(String(value || ""));
      return ["http:", "https:"].includes(parsed.protocol) ? parsed.href : null;
    } catch (_error) {
      return null;
    }
  }

  function mount(root) {
    if (root.dataset.iptMounted === "true") return;
    root.dataset.iptMounted = "true";
    root.classList.add("ipt-public");

    const tournamentIdentifier = String(
      root.dataset.tournamentId
      || script?.dataset?.tournamentId
      || window.IN_PERSON_TOURNAMENT_ID
      || ""
    ).trim();
    const apiBase = String(
      root.dataset.apiBase
      || script?.dataset?.apiBase
      || window.AUTH_BASE_URL
      || DEFAULT_API_BASE
    ).replace(/\/+$/, "");
    const locale = String(root.dataset.locale || document.documentElement.lang || "uk")
      .toLowerCase()
      .startsWith("en") ? "en" : "uk";
    const translate = (uk, en) => locale === "en" ? en : uk;
    const state = { data: null, etag: "", activeTab: "playoffs", loading: false };

    const shell = element("div", "ipt-public-shell");
    const loading = element("div", "ipt-public-state", translate("Завантаження турніру…", "Loading tournament…"));
    root.replaceChildren(loading);

    if (!tournamentIdentifier) {
      loading.classList.add("error");
      loading.textContent = translate(
        "Не вказано ID або slug турніру.",
        "Tournament ID or slug is not configured."
      );
      return;
    }

    function localized(entity, localField = "name_local", englishField = "name_en") {
      if (!entity) return "";
      return String((locale === "uk" ? entity[localField] : null) || entity[englishField] || "");
    }

    function participantName(participantId, match = null, side = null) {
      const participant = (state.data?.players || []).find((entry) => entry.id === participantId);
      if (participant) return localized(participant);
      if (match && side) {
        return String(
          (locale === "uk" ? match[`participant_${side}_name_local`] : null)
          || match[`participant_${side}_name_en`]
          || ""
        );
      }
      return participantId ? String(participantId) : translate("Очікується", "Pending");
    }

    function formatDate(dateValue) {
      if (!dateValue) return "";
      const date = new Date(`${dateValue}T12:00:00Z`);
      if (Number.isNaN(date.getTime())) return String(dateValue);
      return new Intl.DateTimeFormat(locale === "uk" ? "uk-UA" : "en-GB", {
        day: "numeric",
        month: "long",
        year: "numeric",
        timeZone: "UTC",
      }).format(date);
    }

    function formatDateRange(tournament) {
      if (!tournament?.start_date) return "";
      if (tournament.start_date === tournament.end_date) return formatDate(tournament.start_date);
      return `${formatDate(tournament.start_date)} – ${formatDate(tournament.end_date)}`;
    }

    function formatUpdated(value) {
      if (!value) return "";
      const normalized = String(value).includes("T")
        ? String(value)
        : `${String(value).replace(" ", "T")}Z`;
      const date = new Date(normalized);
      if (Number.isNaN(date.getTime())) return String(value);
      return new Intl.DateTimeFormat(locale === "uk" ? "uk-UA" : "en-GB", {
        dateStyle: "medium",
        timeStyle: "short",
      }).format(date);
    }

    function tournamentType(tournament) {
      if (tournament.scope === "international") return translate("Міжнародний турнір", "International tournament");
      if (tournament.local_subtype === "qualifier") return translate("Локальний відбір", "Local qualifier");
      return translate("Національний фінал", "Local final");
    }

    function tournamentLocation(tournament) {
      if (tournament.scope === "international") return translate("Міжнародний", "International");
      const city = localized(tournament, "qualifier_city_name_local", "qualifier_city_name_en");
      return [city, tournament.association_name || tournament.association_id].filter(Boolean).join(", ");
    }

    function statusLabel(status) {
      const labels = {
        registration: ["Реєстрація", "Registration"],
        check_in: ["Реєстрація на місці", "Check-in"],
        swiss: ["Триває швейцарка", "Swiss in progress"],
        playoff: ["Триває плейоф", "Playoff in progress"],
        completed: ["Завершено", "Completed"],
      };
      const label = labels[status] || [String(status || ""), String(status || "")];
      return translate(label[0], label[1]);
    }

    function finishReasonLabel(reason) {
      const labels = {
        time_forfeit: ["прострочення часу", "time forfeit"],
        withdrawal: ["зняття", "withdrawal"],
        disqualification: ["дискваліфікація", "disqualification"],
        no_show: ["неявка", "no-show"],
        admin_decision: ["рішення судді", "admin decision"],
        withdrawn: ["знявся", "withdrawn"],
        disqualified: ["дискваліфікований", "disqualified"],
      };
      const label = labels[reason] || [String(reason || "").replace(/_/g, " "), String(reason || "").replace(/_/g, " ")];
      return translate(label[0], label[1]);
    }

    function createEmpty(message) {
      return element("div", "ipt-public-empty", message);
    }

    function createPanel(tab) {
      const panel = element("section", "ipt-public-panel");
      panel.id = `ipt-public-panel-${tab.id}`;
      panel.setAttribute("role", "tabpanel");
      panel.setAttribute("aria-labelledby", `ipt-public-tab-${tab.id}`);
      const head = element("div", "ipt-public-panel-head");
      head.appendChild(element("h2", "", translate(tab.uk, tab.en)));
      panel.appendChild(head);
      return panel;
    }

    function createMatchPlayers(match) {
      const wrap = element("div");
      (match.is_bye ? ["a"] : ["a", "b"]).forEach((side) => {
        const participantId = match[`participant_${side}_id`];
        const row = element("div", `ipt-public-match-player${match.winner_participant_id === participantId ? " winner" : ""}`);
        const nameWrap = element("div", "ipt-public-match-name");
        nameWrap.appendChild(document.createTextNode(participantName(participantId, match, side)));
        if (participantId && match.starting_participant_id === participantId) {
          nameWrap.appendChild(element("span", "ipt-public-starter", translate("Перший хід", "Starts")));
        }
        row.appendChild(nameWrap);
        let score = "";
        if (match.result_type === "points" && match.status === "completed") {
          score = side === "a" ? match.points_a : match.points_b;
        } else if (match.status === "completed" && match.winner_participant_id === participantId) {
          score = "✓";
        }
        row.appendChild(element("span", "", score));
        wrap.appendChild(row);
      });
      return wrap;
    }

    function matchResultText(match) {
      if (match.is_bye) return translate("Перемога без гри", "Bye win");
      if (match.status !== "completed") return translate("Результат очікується", "Result pending");
      if (match.result_type === "points") return `${match.points_a} : ${match.points_b}`;
      const winner = participantName(match.winner_participant_id, match);
      const reason = finishReasonLabel(match.finish_reason || match.result_type);
      return [winner ? `${winner} — ${translate("перемога", "win")}` : "", reason].filter(Boolean).join(" • ");
    }

    function createMatchCard(match, className = "ipt-public-round-match") {
      const card = element("article", className);
      const head = element("div", "ipt-public-match-head");
      head.appendChild(element("strong", "", match.table_number == null
        ? translate("Без столу", "No table")
        : `${translate("Стіл", "Table")} ${match.table_number}`));
      if (match.table_number === 1) {
        head.appendChild(element("span", "ipt-public-streaming", translate("Трансляційний стіл", "Streaming table")));
      }
      card.appendChild(head);
      card.appendChild(createMatchPlayers(match));
      card.appendChild(element("div", "ipt-public-match-meta", matchResultText(match)));
      return card;
    }

    function renderHeader(container) {
      const tournament = state.data.tournament;
      const header = element("header", "ipt-public-header");
      const row = element("div", "ipt-public-header-row");
      const titleWrap = element("div");
      titleWrap.appendChild(element("h1", "ipt-public-title", localized(tournament)));
      const meta = element("div", "ipt-public-meta");
      appendTextParts(meta, [
        formatDateRange(tournament),
        tournamentType(tournament),
        tournamentLocation(tournament),
        statusLabel(tournament.status),
      ]);
      titleWrap.appendChild(meta);
      row.appendChild(titleWrap);
      const refresh = element("button", "ipt-public-refresh", translate("Оновити", "Refresh"));
      refresh.type = "button";
      refresh.addEventListener("click", () => load({ force: true }));
      row.appendChild(refresh);
      header.appendChild(row);
      const links = element("div", "ipt-public-links");
      appendTextParts(links, [
        tournament.organizer_name ? `${translate("Організатор", "Organizer")}: ${tournament.organizer_name}` : "",
      ]);
      const organizerUrl = safeHttpUrl(tournament.organizer_url);
      const rulesUrl = safeHttpUrl(tournament.rules_url);
      if (organizerUrl) {
        const link = element("a", "", translate("Сайт організатора", "Organizer website"));
        link.href = organizerUrl;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        links.appendChild(link);
      }
      if (rulesUrl) {
        const link = element("a", "", translate("Регламент", "Rules"));
        link.href = rulesUrl;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        links.appendChild(link);
      }
      if (links.childNodes.length) header.appendChild(links);
      const updated = element(
        "div",
        "ipt-public-updated",
        `${translate("Оновлено", "Updated")}: ${formatUpdated(state.data.updated_at)} • revision ${state.data.revision}`
      );
      updated.dataset.iptUpdated = "true";
      header.appendChild(updated);
      container.appendChild(header);
    }

    function renderTabs(container, panels) {
      const tabs = element("div", "ipt-public-tabs");
      tabs.setAttribute("role", "tablist");
      TAB_DEFINITIONS.forEach((tab) => {
        const button = element("button", "ipt-public-tab", translate(tab.uk, tab.en));
        button.type = "button";
        button.id = `ipt-public-tab-${tab.id}`;
        button.setAttribute("role", "tab");
        button.setAttribute("aria-controls", panels.get(tab.id).id);
        button.setAttribute("aria-selected", String(state.activeTab === tab.id));
        button.addEventListener("click", () => selectTab(tab.id, tabs, panels));
        tabs.appendChild(button);
      });
      container.appendChild(tabs);
    }

    function selectTab(tabId, tabs, panels) {
      if (!panels.has(tabId)) return;
      state.activeTab = tabId;
      tabs.querySelectorAll('[role="tab"]').forEach((button) => {
        button.setAttribute("aria-selected", String(button.id === `ipt-public-tab-${tabId}`));
      });
      panels.forEach((panel, id) => {
        panel.hidden = id !== tabId;
      });
      const url = new URL(window.location.href);
      url.searchParams.set("tab", tabId);
      window.history.replaceState({}, "", url.toString());
    }

    function renderPlayoffs(panel) {
      const rounds = state.data.playoff?.rounds || [];
      if (!rounds.length) {
        panel.appendChild(createEmpty(translate("Сітку плейоф ще не опубліковано.", "The playoff bracket has not been published yet.")));
        return;
      }
      const scroll = element("div", "ipt-public-bracket-scroll");
      const bracket = element("div", "ipt-public-bracket");
      rounds.forEach((round) => {
        const column = element("section", "ipt-public-bracket-round");
        column.appendChild(element("h3", "", round.round_label || round.round_key));
        (round.matches || []).forEach((match) => column.appendChild(createMatchCard(match, "ipt-public-bracket-match")));
        bracket.appendChild(column);
      });
      scroll.appendChild(bracket);
      panel.appendChild(scroll);
      const placements = state.data.playoff?.placements;
      if (placements) {
        const line = element("div", "ipt-public-meta");
        appendTextParts(line, [
          `🥇 ${participantName(placements.first)}`,
          `🥈 ${participantName(placements.second)}`,
          `🥉 ${participantName(placements.third)}`,
          `4. ${participantName(placements.fourth)}`,
        ]);
        panel.appendChild(line);
      }
    }

    function renderSwiss(panel) {
      const standings = state.data.swiss?.standings;
      const rows = standings?.rows || [];
      if (!rows.length) {
        panel.appendChild(createEmpty(translate("Standings з’явиться після завершення першого раунду.", "Standings will appear after the first completed round.")));
        return;
      }
      const hint = element("div", "ipt-public-hint", `${translate("Standings revision", "Standings revision")}: ${standings.revision}`);
      panel.appendChild(hint);
      const scroll = element("div", "ipt-public-table-scroll");
      const table = element("table", "ipt-public-table");
      const thead = element("thead");
      const header = element("tr");
      ["#", translate("Гравець", "Player"), translate("Перемоги", "Wins"), "Solkoff1", "Solkoff2", translate("Різниця очок", "VP difference")]
        .forEach((label) => header.appendChild(element("th", "", label)));
      thead.appendChild(header);
      const tbody = element("tbody");
      rows.forEach((standing) => {
        const row = element("tr");
        const vpDifference = Number(standing.vp_difference) > 0
          ? `+${standing.vp_difference}`
          : standing.vp_difference;
        [
          standing.position,
          participantName(standing.participant_id) || localized(standing, "participant_name_local", "participant_name_en"),
          standing.wins,
          standing.solkoff1,
          standing.solkoff2,
          vpDifference,
        ].forEach((value) => row.appendChild(element("td", "", value ?? "")));
        tbody.appendChild(row);
      });
      table.appendChild(thead);
      table.appendChild(tbody);
      scroll.appendChild(table);
      panel.appendChild(scroll);
      panel.appendChild(element(
        "div",
        "ipt-public-hint",
        translate(
          "Порядок: перемоги → Solkoff1 → Solkoff2 → різниця очок.",
          "Order: wins → Solkoff1 → Solkoff2 → VP difference."
        )
      ));
    }

    function renderRounds(panel) {
      const rounds = state.data.swiss?.rounds || [];
      if (!rounds.length) {
        panel.appendChild(createEmpty(translate("Опублікованих Swiss-раундів ще немає.", "No Swiss rounds have been published yet.")));
        return;
      }
      const list = element("div", "ipt-public-rounds");
      rounds.forEach((round, index) => {
        const details = element("details", "ipt-public-round");
        details.open = index === rounds.length - 1;
        const completed = (round.matches || []).filter((match) => match.status === "completed").length;
        details.appendChild(element(
          "summary",
          "",
          `${translate("Раунд", "Round")} ${round.round_number} • ${completed}/${(round.matches || []).length}`
        ));
        const matches = element("div", "ipt-public-round-matches");
        (round.matches || []).forEach((match) => matches.appendChild(createMatchCard(match)));
        details.appendChild(matches);
        list.appendChild(details);
      });
      panel.appendChild(list);
    }

    function renderPlayers(panel) {
      const players = state.data.players || [];
      if (!players.length) {
        panel.appendChild(createEmpty(translate("Список гравців ще не опубліковано.", "The player list has not been published yet.")));
        return;
      }
      const count = element("div", "ipt-public-hint", `${translate("Кількість гравців", "Players")}: ${players.length}`);
      panel.appendChild(count);
      const list = element("div", "ipt-public-players");
      players.slice().sort((left, right) => localized(left).localeCompare(localized(right), locale)).forEach((player) => {
        const card = element("article", "ipt-public-player");
        const line = element("div", "ipt-public-player-line");
        if (player.city_icon_url) {
          const icon = element("img", "ipt-public-location-icon");
          icon.src = player.city_icon_url;
          icon.alt = "";
          icon.loading = "lazy";
          icon.addEventListener("error", () => icon.remove(), { once: true });
          line.appendChild(icon);
        }
        const info = element("div");
        info.appendChild(element("div", "ipt-public-player-name", localized(player)));
        const location = player.city_id
          ? localized(player, "city_name_local", "city_name_en")
          : player.association_name || player.association_id;
        info.appendChild(element("div", "ipt-public-match-meta", [location, player.bga_nickname ? `BGA: ${player.bga_nickname}` : ""].filter(Boolean).join(" • ")));
        line.appendChild(info);
        if (["withdrawn", "disqualified"].includes(player.status)) {
          line.appendChild(element("span", "ipt-public-pill", finishReasonLabel(player.status)));
        }
        card.appendChild(line);
        list.appendChild(card);
      });
      panel.appendChild(list);
    }

    function render() {
      shell.replaceChildren();
      renderHeader(shell);
      const panels = new Map(TAB_DEFINITIONS.map((tab) => [tab.id, createPanel(tab)]));
      const requestedTab = new URL(window.location.href).searchParams.get("tab");
      if (panels.has(requestedTab)) state.activeTab = requestedTab;
      renderTabs(shell, panels);
      renderPlayoffs(panels.get("playoffs"));
      renderSwiss(panels.get("swiss"));
      renderRounds(panels.get("rounds"));
      renderPlayers(panels.get("players"));
      panels.forEach((panel, id) => {
        panel.hidden = id !== state.activeTab;
        shell.appendChild(panel);
      });
      root.replaceChildren(shell);
    }

    function renderError(error) {
      const box = element("div", "ipt-public-state error");
      box.appendChild(element(
        "div",
        "",
        error?.status === 404
          ? translate("Турнір ще не опубліковано або його скасовано.", "The tournament is not published or has been cancelled.")
          : translate("Не вдалося завантажити дані турніру.", "Could not load tournament data.")
      ));
      const retry = element("button", "ipt-public-refresh", translate("Спробувати ще", "Try again"));
      retry.type = "button";
      retry.addEventListener("click", () => load({ force: true }));
      box.appendChild(retry);
      root.replaceChildren(box);
    }

    async function load({ force = false, silent = false } = {}) {
      if (state.loading) return;
      state.loading = true;
      const refreshButton = root.querySelector(".ipt-public-refresh");
      if (refreshButton) refreshButton.disabled = true;
      try {
        const response = await fetch(
          `${apiBase}/public/in-person-tournaments/${encodeURIComponent(tournamentIdentifier)}`,
          {
            credentials: "omit",
            cache: force ? "reload" : "default",
            headers: state.etag ? { "If-None-Match": state.etag } : {},
          }
        );
        if (response.status === 304) {
          const updated = root.querySelector("[data-ipt-updated]");
          if (updated) updated.textContent = `${updated.textContent.split(" • ")[0]} • ${translate("перевірено щойно", "checked just now")}`;
          return;
        }
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || payload?.ok === false) {
          const error = new Error(payload?.message || `Request failed (${response.status})`);
          error.status = response.status;
          throw error;
        }
        state.data = payload;
        state.etag = response.headers.get("ETag") || "";
        render();
      } catch (error) {
        if (!silent || !state.data) renderError(error);
      } finally {
        state.loading = false;
        const nextRefreshButton = root.querySelector(".ipt-public-refresh");
        if (nextRefreshButton) nextRefreshButton.disabled = false;
      }
    }

    load();
    window.setInterval(() => load({ silent: true }), 30000);
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible") load({ silent: true });
    });
  }

  document.querySelectorAll("[data-in-person-tournament]").forEach(mount);
})();
