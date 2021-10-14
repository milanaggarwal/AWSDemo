//
//  MainViewController.swift
//  AWSDemo
//
//  Created by Milan Aggarwal on 14/10/21.
//

import UIKit

class MainViewController : UIViewController {
    var url : URL?
    override func viewDidLoad() {
        url = Bundle.main.url(forResource: "milan", withExtension: "MP4")
        setupViews()
    }
    
    private func setupViews() {
        let button = UIButton()
        button.backgroundColor = .blue
        button.setTitle("Blue pill", for: .normal)
        button.translatesAutoresizingMaskIntoConstraints = false
        self.view.addSubview(button)
        button.addTarget(self, action: #selector(self.singleUpload), for: .touchUpInside)
        button.leadingAnchor.constraint(equalTo: self.view.leadingAnchor, constant: 16).isActive = true
        button.trailingAnchor.constraint(equalTo: self.view.trailingAnchor, constant: -16).isActive = true
        button.centerYAnchor.constraint(equalTo: self.view.centerYAnchor).isActive = true
        button.heightAnchor.constraint(equalToConstant: 100).isActive = true
        
        let button2 = UIButton()
        button2.backgroundColor = .red
        button2.setTitle("Red pill", for: .normal)
        button2.translatesAutoresizingMaskIntoConstraints = false
        button2.addTarget(self, action: #selector(self.multiUpload), for: .touchUpInside)
        self.view.addSubview(button2)
        button2.leadingAnchor.constraint(equalTo: self.view.leadingAnchor, constant: 16).isActive = true
        button2.trailingAnchor.constraint(equalTo: self.view.trailingAnchor, constant: -16).isActive = true
        button2.topAnchor.constraint(equalTo: button.bottomAnchor, constant: 16).isActive = true
        button2.heightAnchor.constraint(equalToConstant: 100).isActive = true
    }
    
    @objc
    private func singleUpload() {
        guard let url = url else { return }
        MultipartRequest(fileURL: url, isSingle: true).start()
    }
    
    @objc
    private func multiUpload() {
        guard let url = url else { return }
        MultipartRequest(fileURL: url, isSingle: false).start()
    }
}
