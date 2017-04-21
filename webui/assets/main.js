class REPL {
    constructor(input, output) {
        this.input = input
        this.output = output
        this.history = []
        this.history_index = 0
        this.history_buffer = ''
        this.result_number = 0
    }
    bind_events() {
        const repl = this
        const keys = {
          ENTER: 13,
          UP_ARROW: 38,
          DOWN_ARROW: 40
        }

        this.input.addEventListener("keydown", (e) => {
          if (e.keyCode == keys.UP_ARROW) {
            e.preventDefault()
            if (this.input.value.substring(0, this.input.selectionStart).indexOf('\n') == '-1') {
                if (this.history_index == 0) {
                    return
                } else {
                    if (this.history_index == this.history.length) {
                        this.history_buffer = this.input.value
                    }
                    this.history_index--
                    this.input.value = this.history[this.history_index]
                    this.input.setSelectionRange(this.input.value.length, this.input.value.length)
                }
            }
          }
          if (e.keyCode == keys.DOWN_ARROW) {
            e.preventDefault()
            if (this.input.value.substring(this.input.selectionEnd, this.input.length).indexOf('\n') == '-1') {
                if (this.history_index == this.history.length) {
                    return
                } else {
                    this.history_index++
                    if (this.history_index == this.history.length) {
                        this.input.value = this.history_buffer
                    } else {
                        this.input.value = this.history[this.history_index]
                    }
                    this.input.setSelectionRange(this.input.value.length, this.input.value.length)
                }
            }
          }
          if (e.keyCode == keys.ENTER && !e.shiftKey) {
            e.preventDefault()
            this.history_buffer = ''
            this.history_index = this.history.length
            this.history[this.history_index] = this.input.value
            this.history_index++
            this.process_query(this.input.value)
            this.input.value = ""
          }
        })
    }

    process_query(query) {
        var xhr = new XMLHttpRequest();
        var dbname = 'foo'; // todo: get db name from dropdown menu
        xhr.open('POST', '/db/' + dbname + '/query');
        xhr.setRequestHeader('Content-Type', 'application/text');

        const repl = this
        xhr.onload = function() {
            repl.result_number++
//const entry = document.createElement('p')
            const result = (
              '<div class="container result">' +
                '<ul class="nav nav-tabs" role="tablist">' +
                  '<li class="nav-item">' +
                    '<a class="nav-link active" data-toggle="tab" href="#result' + repl.result_number + '" role="tab">Result</a>' +
                  '</li>' +
                  '<li class="nav-item">' +
                    '<a class="nav-link" data-toggle="tab" href="#raw' + repl.result_number + '" role="tab">Raw</a>' +
                  '</li>' +
                '</ul>' +
'' +
                '<div class="tab-content">' +
                  '<div class="tab-pane active" id="result' + repl.result_number + '" role="tabpanel">' +
                    '<p class="query">' +
                        query +
                    '</p>' +
                    '<p>Response</p>' +
                    '<p class="response">' +
                      xhr.responseText +
                    '</p>' +
                  '</div>' +
                  '<div class="tab-pane" id="raw' + repl.result_number + '" role="tabpanel">' +
                    '<table class="table table-inverse"><tbody>' +
                    '<tr><th>Response code</th><td>' + xhr.status + '</td></tr>' +
                    '<tr><th>Response text</th><td>' + xhr.responseText + '</td></tr>' +
                    '<tr><th>Response URL</th><td>' + xhr.responseURL + '</td></tr>' +
                    '</tbody></table>' +
                  '</div>' +
                '</div>' +
              '</div>')
            // TODO: remove jquery
            $(repl.output).prepend($(result))
        };
        xhr.send(query);
    }
}

function startup() {
    const input = document.querySelector('.input-textarea')
    const output = document.querySelector('.output')
    repl = new REPL(input, output)
    repl.bind_events()
}
