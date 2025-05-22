#include "server/server.hpp"
#include <iostream>

Server::Server()
    : running_(false)
{
}

Server::~Server()
{
    stop();
}

bool Server::start(unsigned short port)
{
    if (running_) {
        // Server is already running
        return false;
    }

    boost::system::error_code ec;
    try {
        // 1. Acceptor 초기화 및 바인딩
        acceptor = std::make_unique<boost::asio::ip::tcp::acceptor>(io_context, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port));
        
        // 2. 주소 재사용 옵션 설정
        acceptor->set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
        
        // 3. 연결 수락 대기 시작
        acceptor->listen();
        
        // 4. 비동기 연결 수락 시작
        handleNewConnection();
        
        // 5. IO 컨텍스트 실행 스레드 시작
        io_thread = std::thread([this]() { io_context.run(); });
        
        running_ = true;
        std::cout << "Server started on port " << port << std::endl;
        return true;

    } catch (const boost::system::system_error& e) {
        std::cerr << "Server start error: " << e.what() << std::endl;
        running_ = false;
        return false;
    }
}

void Server::stop()
{
    if (!running_) {
        return;
    }

    // 1. Acceptor 닫기 (더 이상 새로운 연결을 받지 않음)
    if (acceptor) {
        boost::system::error_code ec;
        acceptor->close(ec);
        // 오류 처리는 필요에 따라 추가
    }

    // 2. 모든 클라이언트 소켓 닫기
    for (auto const& [clientId, socket] : clients_) {
        if (socket->is_open()) {
            boost::system::error_code ec;
            socket->close(ec);
        }
    }
    clients_.clear();

    // 3. io_context 중지 (진행 중인 모든 비동기 작업 취소)
    io_context.stop();

    // 4. IO 스레드 종료 대기
    if (io_thread.joinable()) {
        io_thread.join();
    }

    running_ = false;
    std::cout << "Server stopped." << std::endl;
}

bool Server::isRunning() const
{
    return running_;
}

void Server::handleNewConnection()
{
    // 1. 새로운 소켓 생성 (새로운 연결을 받을 준비)
    auto new_socket = std::make_shared<boost::asio::ip::tcp::socket>(io_context);
    
    // 2. 비동기 accept 작업 시작
    acceptor->async_accept(*new_socket, 
        [this, new_socket](const boost::system::error_code& ec)
    {
        if (!ec) {
            // 3. 연결 성공 시 처리
            std::string clientId = new_socket->remote_endpoint().address().to_string() + ":" 
                                + std::to_string(new_socket->remote_endpoint().port());
            
            std::cout << "New connection from " << clientId << std::endl;
            
            // 클라이언트 맵에 추가
            clients_[clientId] = new_socket;
            std::cout << "Total connected clients: " << clients_.size() << std::endl;
            
            // 클라이언트로부터 데이터 읽기 시작
            startReading(new_socket, clientId);
            
            // 4. 다음 연결을 위해 다시 accept 대기
            handleNewConnection();
        } else {
            // 5. 오류 발생 시 처리
            std::cerr << "Accept error: " << ec.message() << std::endl;
            if (ec == boost::asio::error::operation_aborted) {
                // 서버가 중지 중인 경우
                return;
            }
            // 다른 오류의 경우 다음 연결 시도
            handleNewConnection();
        }
    });
}

void Server::startReading(std::shared_ptr<boost::asio::ip::tcp::socket> socket, const std::string& clientId)
{
    // 클라이언트로부터 데이터를 읽기 위한 버퍼
    auto buffer = std::make_shared<boost::asio::streambuf>();
    
    // 비동기 읽기 시작
    boost::asio::async_read_until(*socket, *buffer, '\n',
        [this, socket, clientId, buffer](const boost::system::error_code& ec, std::size_t bytes_transferred)
    {
        if (!ec) {
            // 데이터를 성공적으로 읽은 경우
            std::string data(boost::asio::buffers_begin(buffer->data()),
                           boost::asio::buffers_begin(buffer->data()) + bytes_transferred);
            
            // 버퍼에서 읽은 데이터 제거
            buffer->consume(bytes_transferred);
            
            // 데이터 처리
            handleClientData(clientId, data);
            
            // 다음 데이터 읽기 시작
            startReading(socket, clientId);
        } else {
            // 오류 발생 시 (연결 종료 등)
            std::cerr << "Read error from client " << clientId << ": " << ec.message() << std::endl;
            handleClientDisconnection(clientId);
        }
    });
}

void Server::handleClientDisconnection(const std::string& clientId)
{
    // 클라이언트 맵에서 제거
    if (clients_.erase(clientId) > 0) {
        std::cout << "Client " << clientId << " disconnected" << std::endl;
        std::cout << "Remaining clients: " << clients_.size() << std::endl;
    }
}

void Server::handleClientData(const std::string& clientId, const std::string& data)
{
    // 수신된 데이터를 모든 클라이언트에게 브로드캐스트
    std::string message = "[" + clientId + "] " + data;
    for (const auto& [id, socket] : clients_) {
        if (socket->is_open()) {
            boost::asio::write(*socket, boost::asio::buffer(message));
        }
    }
} 