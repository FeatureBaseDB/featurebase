// Copyright 2013 Gary Burd. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package websocket implements the WebSocket protocol defined in RFC 6455.
//
// Overview
//
// The Conn type represents a WebSocket connection. A server application calls
// the Upgrade function from an HTTP request handler to get a pointer to a
// Conn:
//
//  func handler(w http.ResponseWriter, r *http.Request) {
//      ws, err := websocket.Upgrade(w, r, nil, 1024, 1024)
//      if _, ok := err.(websocket.HandshakeError); ok {
//          http.Error(w, "Not a websocket handshake", 400)
//          return
//      } else if err != nil {
//          log.Println(err)
//          return
//      }
//      ... Use conn to send and receive messages.
//  }
//
// Call the connection WriteMessage and ReadMessages methods to send and
// receive messages as a slice of bytes. This snippet of code shows how to echo
// messages using these methods:
//
//  for {
//      messageType, p, err := conn.ReadMessage()
//      if err != nil {
//          return
//      }
//      if err = conn.WriteMessage(messageType, p); err != nil {
//          return err
//      }
//  }
//
// In above snippet of code, p is a []byte and messageType is an int with value
// websocket.BinaryMessage or websocket.TextMessage.
//
// An application can also send and receive messages using the io.WriteCloser
// and io.Reader interfaces. To send a message, call the connection NextWriter
// method to get an io.WriteCloser, write the message to the writer and close
// the writer when done. To receive a message, call the connection NextReader
// method to get an io.Reader and read until io.EOF is returned. This snippet
// snippet shows how to echo messages using the NextWriter and NextReader
// methods:
//
//  for {
//      messageType, r, err := conn.NextReader()
//      if err != nil {
//          return
//      }
//      w, err := conn.NextWriter(messageType)
//      if err != nil {
//          return err
//      }
//      if _, err := io.Copy(w, r); err != nil {
//          return err
//      }
//      if err := w.Close(); err != nil {
//          return err
//      }
//  }
//
// Data Messages
//
// The WebSocket protocol distinguishes between text and binary data messages.
// Text messages are interpreted as UTF-8 encoded text. The interpretation of
// binary messages is left to the application.
//
// This package uses the TextMessage and BinaryMessage integer constants to
// identify the two data message types. The ReadMessage and NextReader methods
// return the type of the received message. The messageType argument to the
// WriteMessage and NextWriter methods specifies the type of a sent message.
//
// It is the application's responsibility to ensure that text messages are
// valid UTF-8 encoded text.
//
// Control Messages
//
// The WebSocket protocol defines three types of control messages: close, ping
// and pong. Call the connection WriteControl, WriteMessage or NextWriter
// methods to send a control message to the peer.
//
// Connections handle received ping and pong messages by invoking a callback
// function set with SetPingHandler and SetPongHandler methods. These callback
// functions can be invoked from the ReadMessage method, the NextReader method
// or from a call to the data message reader returned from NextReader.
//
// Connections handle received close messages by returning an error from the
// ReadMessage method, the NextReader method or from a call to the data message
// reader returned from NextReader.
//
// Concurrency
//
// A Conn supports a single concurrent caller to the write methods (NextWriter,
// SetWriteDeadline, WriteMessage) and a single concurrent caller to the read
// methods (NextReader, SetReadDeadline, ReadMessage). The Close and
// WriteControl methods can be called concurrently with all other methods.
//
package websocket
